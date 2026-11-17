import argparse
import ipaddress
import json
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument('--base-url', required=True)
parser.add_argument('--chromium', required=True)
parser.add_argument('--artifacts', required=True)
parser.add_argument('--role', choices=['master', 'regular'], required=True)
parser.add_argument('--profile', required=True)
args = parser.parse_args()
base = urlparse(args.base_url)
assert base.scheme == 'http' and ipaddress.ip_address(base.hostname).is_loopback
out = Path(args.artifacts)
out.mkdir(parents=True, exist_ok=True)
prefix = '/admin' if args.role == 'master' else '/account'
path = prefix + '/kids-settings?profileId=' + args.profile
mode_path = prefix + '/api/users/' + args.profile + '/kids/mode'
pattern = '**' + mode_path
password = 'synthetic-parent-password'

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox', '--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    errors, denied = [], []

    def guard(route):
        target = urlparse(route.request.url)
        if target.scheme == 'http' and target.netloc == base.netloc:
            route.continue_()
        else:
            denied.append(target.scheme + ':blocked')
            route.abort()

    context.route('**/*', guard)
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    button = lambda: page.get_by_role('button', name='Save Mode', exact=True)

    def loaded():
        page.goto(args.base_url + path)
        button().wait_for()
        page.wait_for_function('!document.querySelector("#save-mode").disabled')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert page.evaluate('typeof window.parentModeFetch') == 'function'

    def start():
        before = state()
        desired = 'content_list' if before['mode'] == 'rating' else 'rating'
        page.locator('input[name="kidsMode"][value="' + desired + '"]').check()
        button().click()
        dialog().wait_for()
        assert dialog().count() == 1
        assert state()['writes'] == before['writes']
        return before, desired

    def confirm():
        dialog().locator('input[type="password"]').fill(password)
        dialog().get_by_role('button', name='Confirm', exact=True).click()
        dialog().wait_for(state='detached')

    def no_retry(before):
        page.wait_for_timeout(100)
        after = state()
        assert after['writes'] == before['writes']
        assert after['retries'] == before['retries']
        assert after['kids'] is True

    for width in [1280, 390]:
        page.set_viewport_size({'width': width, 'height': 900 if width == 1280 else 844})
        loaded()
        before, _ = start()
        if width == 1280:
            dialog().get_by_role('button', name='Cancel', exact=True).click()
        else:
            page.keyboard.press('Escape')
        dialog().wait_for(state='detached')
        no_retry(before)
        page.wait_for_function('!document.querySelector("#save-mode").disabled')
        before, desired = start()
        assert dialog().evaluate('(element)=>element.getBoundingClientRect().left>=0&&element.getBoundingClientRect().right<=innerWidth')
        dialog().screenshot(path=str(out / ('confirm-' + str(width) + '.png')))
        confirm()
        page.get_by_text('Mode saved', exact=True).wait_for()
        after = state()
        assert after['writes'] == before['writes'] + 1 and after['mode'] == desired
        assert after['retries'] == before['retries'] + 1 and after['kids'] is True
        assert password not in page.content()

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/rotate').status == 204
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.', exact=True).wait_for()
    assert state()['writes'] == before['writes']
    assert state()['retries'] == before['retries'] + 1

    loaded()
    before, _ = start()
    page.evaluate("history.replaceState({}, '', '?profileId=different-profile')")
    confirm()
    no_retry(before)

    loaded()
    before, desired = start()
    page.evaluate("""desired => {
        const replacement = desired === 'rating' ? 'content_list' : 'rating';
        const radio = document.querySelector('input[name="kidsMode"][value="' + replacement + '"]');
        radio.checked = true; radio.dispatchEvent(new Event('change', {bubbles:true}));
    }""", desired)
    dialog().wait_for(state='detached')
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("const original=document.querySelector('#kids-mode-form');original.replaceWith(original.cloneNode(true))")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.goto(args.base_url + '/fixture/away')
    no_retry(before)

    loaded()
    held = []

    def hold_first(route):
        held.append((route, route.fetch()))

    page.route(pattern, hold_first)
    before = state()
    desired = 'content_list' if before['mode'] == 'rating' else 'rating'
    page.locator('input[name="kidsMode"][value="' + desired + '"]').check()
    button().click()
    page.wait_for_function('document.querySelector("#save-mode").disabled')
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held
    page.goto(args.base_url + '/fixture/away')
    route, response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass  # The original action was canceled on actual navigation.
    page.unroute(pattern, hold_first)
    assert dialog().count() == 0
    no_retry(before)

    # This is the actual local maximum prompt wait. The server independently
    # retains its own earlier capture deadline; no client clock is an authority.
    loaded()
    before, _ = start()
    dialog().wait_for(state='detached', timeout=33000)
    no_retry(before)

    loaded()
    held = []

    def hold_success(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            held.append((route, response))
        else:
            route.fulfill(response=response)

    page.route(pattern, hold_success)
    before, desired = start()
    confirm()
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held and state()['writes'] == before['writes'] + 1
    page.goto(args.base_url + '/fixture/away')
    route, response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass
    page.unroute(pattern, hold_success)
    assert state()['mode'] == desired
    assert 'Mode saved' not in page.content()

    loaded()

    def unknown_after_actual_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=503, content_type='application/json', body=json.dumps({'code':'parent_confirmation_outcome_unknown','error':'PRIVATE_RESPONSE_CANARY'}))
        else:
            route.fulfill(response=response)

    page.route(pattern, unknown_after_actual_commit)
    before, desired = start()
    confirm()
    page.get_by_text('The result is not known. Reload settings before trying again.', exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1 and state()['mode'] == desired
    assert button().is_disabled() and 'PRIVATE_RESPONSE_CANARY' not in page.content()
    attempts = state()['attempts']
    page.evaluate("document.querySelector('input[name=kidsMode]').dispatchEvent(new Event('change'))")
    assert button().is_disabled() and state()['attempts'] == attempts
    page.unroute(pattern, unknown_after_actual_commit)
    loaded()

    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/retire-actor').status == 204
    confirm()
    page.wait_for_timeout(100)
    assert state()['writes'] == before['writes']
    assert page.get_by_text('Mode saved', exact=True).count() == 0
    assert not errors and not denied
    (out / 'result.json').write_text(json.dumps({
        'fixture':'actual-cookie-templates-asset-own-schema-pg-parent-mode', 'role':args.role, 'widths':[1280,390],
        'pageErrors':errors, 'blockedPageRequests':denied, 'freshMasterProof':args.role=='master',
        'oneDialogPerExplicitAction':True, 'cancelEscapeNoRetry':True, 'cookieRotationDenied':True,
        'profileModeFormPageRetirementNoRetry':True, 'lateFirstChallengeIgnored':True,
        'localPromptWaitBounded':True, 'lateActualCommitPreservedWithoutDOMSuccess':True,
        'unknownResponseAfterActualCommitStopsUntilReload':True, 'currentActorRetirementDenied':True,
        'unknownFixtureScope':'HTTP reply replacement after known actual commit; genuine datastore COMMIT uncertainty is a separate server test.'
    }, indent=2) + '\n')
    browser.close()
