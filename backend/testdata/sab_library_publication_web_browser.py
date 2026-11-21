"""Exercise actual saved-stage publication through the production admin UI.

Only negative UI validation and loss/delay of genuine server responses are
intercepted. Publication, copying, status and reconciliation use the original
current PG cookie actor, saved stage, files and production publisher.
"""
import argparse
import copy
import json
import time
from pathlib import Path
from contextlib import ExitStack
from urllib.parse import urlparse

from playwright.sync_api import Error as PlaywrightError, sync_playwright


parser = argparse.ArgumentParser()
parser.add_argument("--base-url", required=True)
parser.add_argument("--unknown-url", required=True)
parser.add_argument("--reconcile-url", required=True)
parser.add_argument("--chromium", required=True)
parser.add_argument("--artifacts", required=True)
parser.add_argument("--case", choices=("happy", "unknown", "reconcile"))
args = parser.parse_args()


def origin(value):
    parsed = urlparse(value)
    assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
    assert parsed.port and not parsed.username and not parsed.password
    assert parsed.path in ("", "/") and not parsed.query and not parsed.fragment
    return "http://127.0.0.1:" + str(parsed.port)


bases = [origin(args.base_url), origin(args.unknown_url), origin(args.reconcile_url)]
allowed = set(bases)
artifacts = Path(args.artifacts)
assert artifacts.is_absolute()
artifacts.mkdir(parents=True, exist_ok=True)
errors, blocked = [], []


def request_origin(url):
    parsed = urlparse(url)
    if parsed.scheme != "http" or parsed.username or parsed.password:
        return ""
    return parsed.scheme + "://" + parsed.netloc


def guard(route):
    if request_origin(route.request.url) in allowed:
        route.continue_()
    else:
        blocked.append("Unexpected browser destination blocked")
        route.abort("blockedbyclient")


def wait_for(page, predicate, description):
    deadline = time.monotonic() + 12
    while not predicate():
        assert time.monotonic() < deadline, description
        page.wait_for_timeout(20)


def state(context, base):
    # APIRequestContext bypasses page routing, so every helper has its own
    # strict original-origin construction and disables redirects.
    assert base in allowed
    reply = context.request.get(base + "/app/fixture/state", max_redirects=0)
    assert reply.status == 200
    value = reply.json()
    for key in ("items", "records", "copyOperations", "physicalFiles", "deniedHTTP",
                "stageGets", "stagePosts", "previewPosts", "publishPosts",
                "reconcilePosts", "statusGets", "librariesGets", "refreshPosts"):
        assert type(value[key]) is int and value[key] >= 0, (key, value)
    assert isinstance(value["fingerprint"], str)
    assert value["deniedHTTP"] == 0, "Fixture attempted an unapproved HTTP destination"
    return value


def no_publication(value):
    assert value["items"] == value["records"] == value["copyOperations"] == value["physicalFiles"] == 0, value
    assert value["publishPosts"] == value["reconcilePosts"] == value["stagePosts"] == value["refreshPosts"] == 0, value


def one_publication(value):
    assert value["items"] == value["records"] == value["copyOperations"] == value["physicalFiles"] == 1, value
    assert value["stagePosts"] == value["refreshPosts"] == 0, value


def fixture_post(context, base, suffix):
    assert base in allowed and suffix in ("prepare-unregistered", "demote")
    reply = context.request.post(
        base + "/app/fixture/" + suffix,
        headers={"Origin": base, "Content-Type": "application/json"},
        data={"libraryId": "publication-library", "ordinals": [1]},
        max_redirects=0,
    )
    assert reply.status in (200, 204), (suffix, reply.status)


def panel_ready(page):
    panel = page.locator("#sabFallbackPanel")
    panel.get_by_text("Saved observations. No provider check was started.", exact=True).wait_for()
    assert panel.locator("article").count() == 1
    card = panel.locator("article").first
    assert card.locator("h3").inner_text() == "Queue 1"
    return panel, card


def view_saved(page):
    panel, card = panel_ready(page)
    card.get_by_role("button", name="View saved copy", exact=True).click()
    card.get_by_text("A saved copy was recorded. This does not mean ready to play.", exact=False).wait_for()
    assert card.get_by_role("button", name="Copy files", exact=True).is_hidden()
    assert card.locator("[data-sab-library-publication]").count() == 1
    return panel, card


def all_card_controls_locked(card):
    assert card.evaluate("""card => Array.from(card.querySelectorAll('button,input,select'))
        .every(node => node.disabled || Boolean(node.closest('[inert]')))"""), "A competing copy/preview/library action stayed enabled"


def hold_genuine(page, pattern, button, card, mutation=None):
    held = []

    def hold(route):
        # The original request runs to its real current server outcome.
        response = route.fetch(max_redirects=0)
        held.append((route, response))

    page.route(pattern, hold)
    if mutation:
        page.once("dialog", lambda dialog: dialog.accept())
    button.click()
    wait_for(page, lambda: bool(held), "Genuine response was not held")
    assert len(held) == 1
    all_card_controls_locked(card)
    return held, hold


def release_genuine(page, pattern, held, callback):
    route, response = held.pop()
    assert response.status == 200, response.status
    route.fulfill(response=response)
    page.unroute(pattern, callback)


def choose_library(page, card, base, inspect_locks=False):
    button = card.get_by_role("button", name="Choose library", exact=True)
    if inspect_locks:
        pattern = base + "/app/admin/api/library/libraries"
        held, callback = hold_genuine(page, pattern, button, card)
        release_genuine(page, pattern, held, callback)
    else:
        button.click()
    card.get_by_text("No library import is recorded. Select a target and inspect the saved files.", exact=True).wait_for()
    selection = card.get_by_role("combobox", name="Target library", exact=True)
    assert selection.input_value() == "publication-library"
    assert card.get_by_role("button", name="Inspect saved files", exact=True).is_enabled()
    assert card.get_by_role("button", name="Add selected files", exact=True).is_disabled()


def inspect_files(page, card, base, lock=False):
    button = card.get_by_role("button", name="Inspect saved files", exact=True)
    if lock:
        pattern = base + "/app/admin/api/usenet/fallback/1/publication/preview"
        held, callback = hold_genuine(page, pattern, button, card)
        release_genuine(page, pattern, held, callback)
    else:
        button.click()
    card.get_by_text("Choose the files to add. Titles are not matched automatically.", exact=True).wait_for()
    choice = card.get_by_role("checkbox")
    assert choice.count() == 1 and choice.is_checked() and choice.is_enabled()
    assert "Matroska" in card.inner_text()
    assert card.get_by_role("button", name="Add selected files", exact=True).is_enabled()


def no_script_or_evidence(page, panel):
    assert not panel.locator("img").count()
    assert "sha256" not in panel.inner_text() and ".mediastorm-publications" not in panel.inner_text()
    assert page.evaluate("window.PUBLICATION_XSS") is None


def recorded(page, card):
    card.get_by_text("The files were added to the library. Review their title matches in Libraries.", exact=True).wait_for()
    assert "File 1: Added; title not matched." in card.inner_text()
    assert card.get_by_role("button", name="Add selected files", exact=True).is_disabled()
    assert card.get_by_role("button", name="Verify recorded import", exact=True).is_disabled()
    select = card.get_by_role("combobox", name="Target library", exact=True)
    assert select.input_value() == "publication-library" and select.is_disabled()


def current_status(context, base):
    reply = context.request.get(base + "/app/admin/api/usenet/fallback/1/publication", max_redirects=0)
    assert reply.status == 200
    value = reply.json()
    assert value["libraryId"] == "publication-library" and value["ordinals"] == [1]
    assert value["state"] == "registered" and len(value["files"]) == 1
    assert value["files"][0]["matchStatus"] == "unmatched"
    assert not any(key in value for key in ("path", "sha256", "receipt", "token"))
    return value


with sync_playwright() as pw, ExitStack() as cleanup:
    browser = pw.chromium.launch(
        executable_path=args.chromium, headless=True,
        args=["--no-sandbox", "--disable-background-networking", "--disable-component-update",
              "--disable-sync", "--no-first-run"],
    )
    cleanup.callback(browser.close)
    contexts = []

    def new_page(base):
        # Cookies are host/path scoped, not port scoped. Separate contexts keep
        # the three real original actor cookies from replacing one another.
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        contexts.append(context)
        cleanup.callback(context.close)
        context.route("**/*", guard)
        page = context.new_page()
        page.set_default_timeout(12000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        assert context.request.get(base + "/app/fixture/sign-in", max_redirects=0).status == 204
        baseline = state(context, base)
        no_publication(baseline)
        assert baseline["stageGets"] == baseline["previewPosts"] == baseline["statusGets"] == baseline["librariesGets"] == 0
        page.goto(base + "/app/admin/tools")
        # Existing Tools initialization legitimately reads this same admin
        # library route after its profile load. Wait for the actual response
        # consumer's DOM result, not a timer or an assumed network baseline.
        page.locator('#newTaskLocalMediaLibrary option[value="publication-library"]').wait_for(state="attached")
        initialized = state(context, base)
        assert initialized["librariesGets"] == 1
        no_publication(initialized)
        return context, page

    results = {}
    if args.case in (None, "happy"):
        base = bases[0]
        context, page = new_page(base)
        initial = state(context, base)
        no_publication(initial)
        assert initial["stageGets"] == initial["previewPosts"] == initial["statusGets"] == 0
        assert initial["librariesGets"] == 1
        panel, card = view_saved(page)
        passive = state(context, base)
        no_publication(passive)
        assert passive["stageGets"] == 1 and passive["previewPosts"] == 0

        # A forged endpoint is refused before any foreign request is sent.
        original_library_endpoint = panel.get_attribute("data-library-endpoint")
        panel.evaluate("node => node.dataset.libraryEndpoint = 'http://foreign.invalid/library'")
        card.get_by_role("button", name="View saved copy", exact=True).click()
        card.get_by_text("A saved copy was recorded. This does not mean ready to play.", exact=False).wait_for()
        assert card.locator("[data-sab-library-publication]").count() == 0
        assert state(context, base)["librariesGets"] == initial["librariesGets"]
        panel.evaluate("(node, value) => node.dataset.libraryEndpoint = value", original_library_endpoint)
        card.get_by_role("button", name="View saved copy", exact=True).click()
        card.locator("[data-sab-library-publication]").wait_for()
        choose_library(page, card, base, inspect_locks=True)
        no_publication(state(context, base))

        # Only malformed descriptive UI responses are synthesized. Each starts
        # from a genuine, fully completed preview of the real saved bytes.
        preview_pattern = base + "/app/admin/api/usenet/fallback/1/publication/preview"
        malformed = ("future-field", "wrong-library", "wrong-ordinal", "invalid-format", "unsafe-bytes")
        for kind in malformed:
            seen = []

            def malformed_reply(route, kind=kind):
                response = route.fetch(max_redirects=0)
                assert response.status == 200
                value = copy.deepcopy(response.json())
                if kind == "future-field":
                    value["files"][0]["path"] = '<img src=x onerror="window.PUBLICATION_XSS=true">'
                elif kind == "wrong-library":
                    value["libraryId"] = "http://foreign.invalid/library"
                elif kind == "wrong-ordinal":
                    value["files"][0]["ordinal"] = 2
                elif kind == "invalid-format":
                    value["files"][0]["container"] = "external-manifest"
                else:
                    value["files"][0]["bytes"] = -1
                seen.append(True)
                route.fulfill(status=200, content_type="application/json", body=json.dumps(value))

            page.route(preview_pattern, malformed_reply)
            card.get_by_role("button", name="Inspect saved files", exact=True).click()
            card.get_by_text("The saved files could not be inspected. No library import was requested.", exact=True).wait_for()
            assert seen and card.get_by_role("button", name="Add selected files", exact=True).is_disabled()
            assert card.get_by_role("checkbox").count() == 0
            no_script_or_evidence(page, panel)
            no_publication(state(context, base))
            page.unroute(preview_pattern, malformed_reply)

        # A late actual preview from the previous document cannot re-enable its
        # old selection after a page reload.
        held, callback = hold_genuine(page, preview_pattern, card.get_by_role("button", name="Inspect saved files", exact=True), card)
        page.reload()
        panel, card = panel_ready(page)
        route, response = held.pop()
        try:
            route.fulfill(response=response)
        except PlaywrightError:
            pass  # The real old request was canceled by document retirement.
        page.unroute(preview_pattern, callback)
        assert card.locator("[data-sab-library-publication]").count() == 0
        assert card.get_by_role("checkbox").count() == 0
        no_publication(state(context, base))
        panel, card = view_saved(page)
        choose_library(page, card, base)
        inspect_files(page, card, base, lock=True)
        before_publish = state(context, base)
        no_publication(before_publish)
        publish_pattern = base + "/app/admin/api/usenet/fallback/1/publication"
        held, callback = hold_genuine(page, publish_pattern, card.get_by_role("button", name="Add selected files", exact=True), card, mutation=True)
        committed = state(context, base)
        one_publication(committed)
        assert committed["publishPosts"] == 1 and committed["reconcilePosts"] == 0
        release_genuine(page, publish_pattern, held, callback)
        recorded(page, card)
        no_script_or_evidence(page, panel)
        page.screenshot(path=str(artifacts / "publication-1280.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        if page.locator("#adminSidebar").count():
            page.wait_for_function("document.querySelector('#adminSidebar').getBoundingClientRect().right <= 0")
        panel.scroll_into_view_if_needed()
        assert panel.evaluate("node => node.getBoundingClientRect().left >= 0 && node.getBoundingClientRect().right <= innerWidth")
        page.screenshot(path=str(artifacts / "publication-390.png"), full_page=True)
        current_status(context, base)

        # An actual current role change retires all controls, even with a saved
        # previously registered operation. No new mutation is automatically sent.
        fixture_post(context, base, "demote")
        card.get_by_role("button", name="Check library import", exact=True).click()
        panel.get_by_text("Access is no longer available. Reload the page after signing in.", exact=True).wait_for()
        assert panel.locator("article").count() == 0
        assert panel.get_by_role("button", name="Reload saved status", exact=True).is_disabled()
        after_demote = state(context, base)
        one_publication(after_demote)
        assert after_demote["publishPosts"] == 1 and after_demote["reconcilePosts"] == 0

        results["happy"] = after_demote

    if args.case in (None, "unknown"):
        # Lose only the genuine successful client response, after the server's
        # original copy/registration/known ACK has physically completed.
        unknown_base = bases[1]
        unknown_context, unknown_page = new_page(unknown_base)
        unknown_panel, unknown_card = view_saved(unknown_page)
        choose_library(unknown_page, unknown_card, unknown_base)
        inspect_files(unknown_page, unknown_card, unknown_base)
        lost = []
        unknown_pattern = unknown_base + "/app/admin/api/usenet/fallback/1/publication"

        def lose_actual_ack(route):
            assert route.request.method == "POST"
            response = route.fetch(max_redirects=0)
            assert response.status == 200 and response.json()["state"] == "registered"
            lost.append(response.json()["reference"])
            route.abort("failed")

        unknown_page.route(unknown_pattern, lose_actual_ack)
        unknown_page.once("dialog", lambda dialog: dialog.accept())
        unknown_card.get_by_role("button", name="Add selected files", exact=True).click()
        unknown_card.get_by_text("The import could not be confirmed and may have completed. Use Check library import before taking further action.", exact=True).wait_for()
        assert len(lost) == 1
        unknown_page.unroute(unknown_pattern, lose_actual_ack)
        assert unknown_card.get_by_role("button", name="Add selected files", exact=True).is_disabled()
        assert unknown_card.get_by_role("button", name="Verify recorded import", exact=True).is_disabled()
        uncertain = state(unknown_context, unknown_base)
        one_publication(uncertain)
        assert uncertain["publishPosts"] == 1 and uncertain["reconcilePosts"] == 0
        unknown_card.get_by_role("button", name="Check library import", exact=True).click()
        recorded(unknown_page, unknown_card)
        assert current_status(unknown_context, unknown_base)["reference"] == lost[0]
        unknown_page.reload()
        unknown_panel, unknown_card = view_saved(unknown_page)
        unknown_card.get_by_role("button", name="Check library import", exact=True).click()
        recorded(unknown_page, unknown_card)
        restored = state(unknown_context, unknown_base)
        one_publication(restored)
        assert restored["publishPosts"] == 1 and restored["reconcilePosts"] == 0
        assert restored["fingerprint"] == uncertain["fingerprint"]
        current_status(unknown_context, unknown_base)
        no_script_or_evidence(unknown_page, unknown_panel)

        results["unknown"] = restored

    if args.case in (None, "reconcile"):
        # Reconcile starts with an actual source-owned durable copy and reserve,
        # never with a fake catalogue item/result or a posted filesystem receipt.
        reconcile_base = bases[2]
        reconcile_context, reconcile_page = new_page(reconcile_base)
        unprepared = state(reconcile_context, reconcile_base)
        no_publication(unprepared)
        fixture_post(reconcile_context, reconcile_base, "prepare-unregistered")
        prepared = state(reconcile_context, reconcile_base)
        assert prepared["items"] == 0 and prepared["records"] == prepared["copyOperations"] == prepared["physicalFiles"] == 1
        assert prepared["publishPosts"] == prepared["reconcilePosts"] == 0
        assert prepared["fingerprint"]
        reconcile_panel, reconcile_card = view_saved(reconcile_page)
        reconcile_card.get_by_role("button", name="Check library import", exact=True).click()
        reconcile_card.get_by_text("The copied files are saved. Verify the recorded import to check and finish library registration.", exact=True).wait_for()
        assert reconcile_card.get_by_role("button", name="Add selected files", exact=True).is_disabled()
        verify = reconcile_card.get_by_role("button", name="Verify recorded import", exact=True)
        assert verify.is_enabled()
        reconcile_pattern = reconcile_base + "/app/admin/api/usenet/fallback/1/publication/reconcile"
        held, callback = hold_genuine(reconcile_page, reconcile_pattern, verify, reconcile_card, mutation=True)
        reconciled = state(reconcile_context, reconcile_base)
        one_publication(reconciled)
        assert reconciled["publishPosts"] == 0 and reconciled["reconcilePosts"] == 1
        assert reconciled["fingerprint"] == prepared["fingerprint"]
        release_genuine(reconcile_page, reconcile_pattern, held, callback)
        recorded(reconcile_page, reconcile_card)
        current_status(reconcile_context, reconcile_base)
        no_script_or_evidence(reconcile_page, reconcile_panel)

        results["reconcile"] = reconciled

    assert not errors, errors
    assert not blocked, blocked
    final = {
        "fixture": "actual-current-PG-master-saved-stage-original-copy-and-registration",
        "case": args.case or "all", "executedCases": list(results),
        "widths": [1280, 390] if "happy" in results else [1280],
        "pageErrors": errors, "blockedPageRequests": blocked,
        "negativePreviewResponsesRejected": len(malformed) if "happy" in results else 0,
        "offOriginEndpointDenied": "happy" in results,
        "oldDocumentPreviewIgnored": "happy" in results,
        "lockedCompetingControls": "happy" in results or "reconcile" in results,
        "happy": results.get("happy"), "lostClientACK": results.get("unknown"),
        "reconcile": results.get("reconcile"),
        "automaticPublicationRetries": 0, "titleAssociation": "unmatched",
    }
    (artifacts / "result.json").write_text(json.dumps(final, indent=2) + "\n")
