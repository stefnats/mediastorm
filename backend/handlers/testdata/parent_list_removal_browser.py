import argparse
import ipaddress
import json
import re
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
mode_path = prefix + '/api/users/' + args.profile + '/kids/lists'
pattern = '**' + mode_path + '?*'
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
    native_choices, native_questions = [], []

    def native_question(question):
        assert question.type == 'confirm' and question.message == 'Remove this list?'
        native_questions.append(question.message)
        if native_choices and not native_choices.pop(0):
            question.dismiss()
        else:
            question.accept()

    page.on('dialog', native_question)
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    button = lambda: page.locator('#lists-container .remove-allowed-list').first

    def loaded():
        assert context.request.get(args.base_url + '/fixture/seed').status == 204
        page.goto(args.base_url + path)
        button().wait_for()
        page.wait_for_function('!document.querySelector(".remove-allowed-list").disabled')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert page.evaluate('typeof window.parentListRemovalFetch') == 'function'
        assert not page.evaluate('Boolean(window.__listRemovalInjected)')
        assert not page.evaluate('Boolean(window.__listRemovalScheme)')
        assert page.locator('#lists-container button[onclick]').count() == 0
        for value in ['javascript:window.__listRemovalScheme=1', 'http://[']:
            anchor = page.locator('#lists-container a').filter(has_text=re.compile('^' + re.escape(value) + '$'))
            assert anchor.count() == 1 and anchor.get_attribute('href') is None
        assert page.locator('#lists-container a[href]').count() == 3

    def start():
        before = state()
        identity = before['lists'][0]
        assert page.locator('#lists-container a').first.text_content() == identity
        button().click()
        dialog().wait_for()
        assert dialog().count() == 1
        assert state()['writes'] == before['writes']
        return before, identity

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

    loaded()
    before = state()
    native_choices.append(False)
    button().click()
    no_retry(before)
    assert state()['attempts'] == before['attempts'] and dialog().count() == 0

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
        page.wait_for_function('!document.querySelector(".remove-allowed-list").disabled')
        before, identity = start()
        assert dialog().evaluate('(element)=>element.getBoundingClientRect().left>=0&&element.getBoundingClientRect().right<=innerWidth')
        dialog().screenshot(path=str(out / ('confirm-' + str(width) + '.png')))
        confirm()
        page.get_by_text('List removed', exact=True).wait_for()
        after = state()
        assert after['writes'] == before['writes'] + 1
        assert after['lists'] == [value for value in before['lists'] if value != identity]
        assert after['retries'] == before['retries'] + 1 and after['kids'] is True
        assert after['mode'] == before['mode'] == 'content_list'
        assert password not in page.content() and not page.evaluate('Boolean(window.__listRemovalInjected)')
        assert page.locator('#lists-container a').count() == len(after['lists'])
        # Non-HTTP stored text is inert but keeps the identical removable value.
        value = 'javascript:window.__listRemovalScheme=1'
        anchor = page.locator('#lists-container a').filter(has_text=re.compile('^' + re.escape(value) + '$'))
        anchor.click()
        assert not page.evaluate('Boolean(window.__listRemovalScheme)')
        before = state()
        anchor.locator('xpath=../..').get_by_role('button', name='Remove', exact=True).click()
        dialog().wait_for()
        assert dialog().count() == 1
        confirm()
        page.wait_for_function('!profile.kidsAllowedLists.includes("javascript:window.__listRemovalScheme=1")')
        assert state()['lists'] == [item for item in before['lists'] if item != value]
        assert state()['writes'] == before['writes'] + 1 and not page.evaluate('Boolean(window.__listRemovalScheme)')

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/rotate').status == 204
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.', exact=True).wait_for()
    assert state()['writes'] == before['writes'] and state()['retries'] == before['retries'] + 1

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/mutate-list').status == 204
    changed = state()['lists']
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.', exact=True).wait_for()
    assert state()['writes'] == before['writes'] and state()['lists'] == changed

    loaded()
    before, _ = start()
    page.evaluate("history.replaceState({}, '', '?profileId=different-profile')")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("const row=document.querySelector('#lists-container .list-item');row.replaceWith(row.cloneNode(true))")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("profile.kidsAllowedLists.push('synthetic-changed-local-array')")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("const radio=document.querySelector('input[name=kidsMode][value=rating]');radio.checked=true;radio.dispatchEvent(new Event('change',{bubbles:true}))")
    dialog().wait_for(state='detached')
    no_retry(before)

    loaded()
    before, _ = start()
    page.goto(args.base_url + '/fixture/away')
    no_retry(before)

    # Seven unclaimed canceled challenges above plus this eighth challenge fit
    # the real per-actor registry budget; no test-only registry reset is used.
    loaded()
    before, _ = start()
    dialog().wait_for(state='detached', timeout=33000)
    no_retry(before)

    loaded()
    held = []

    def hold_first(route):
        held.append((route, route.fetch()))

    page.route(pattern, hold_first)
    before = state()
    button().click()
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
        pass  # Actual navigation canceled the old action.
    page.unroute(pattern, hold_first)
    assert dialog().count() == 0
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
    before, identity = start()
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
    assert state()['lists'] == [value for value in before['lists'] if value != identity]
    assert 'List removed' not in page.content()

    loaded()

    def unknown_after_actual_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=503, content_type='application/json', body=json.dumps({'code':'parent_confirmation_outcome_unknown','error':'PRIVATE_REPLY_CANARY'}))
        else:
            route.fulfill(response=response)

    page.route(pattern, unknown_after_actual_commit)
    before, identity = start()
    confirm()
    page.get_by_text('The result is not known. Reload settings before trying again.', exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1
    assert state()['lists'] == [value for value in before['lists'] if value != identity]
    assert all(control.is_disabled() for control in page.locator('.remove-allowed-list').all())
    assert 'PRIVATE_REPLY_CANARY' not in page.content()
    attempts = state()['attempts']
    page.evaluate("document.querySelector('input[name=kidsMode]').dispatchEvent(new Event('change'))")
    assert all(control.is_disabled() for control in page.locator('.remove-allowed-list').all())
    assert state()['attempts'] == attempts
    page.unroute(pattern, unknown_after_actual_commit)

    loaded()

    def malformed_after_actual_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=200, content_type='application/json', body=json.dumps({'id':args.profile,'kidsAllowedLists':None}))
        else:
            route.fulfill(response=response)

    page.route(pattern, malformed_after_actual_commit)
    before, _ = start()
    confirm()
    page.get_by_text('The result could not be confirmed. Reload settings before trying again.', exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1
    assert all(control.is_disabled() for control in page.locator('.remove-allowed-list').all())
    page.unroute(pattern, malformed_after_actual_commit)

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/retire-actor').status == 204
    confirm()
    page.wait_for_timeout(100)
    assert state()['writes'] == before['writes']
    assert page.get_by_text('List removed', exact=True).count() == 0
    assert not errors and not denied
    (out / 'result.json').write_text(json.dumps({
        'fixture':'actual-cookie-templates-asset-own-schema-pg-parent-list-removal', 'role':args.role, 'widths':[1280,390],
        'pageErrors':errors, 'blockedPageRequests':denied, 'freshMasterProof':args.role=='master',
        'quoteUnicodeLiteralPercentPlusClosure':True, 'allExactDuplicatesRemovedOtherRowsRetained':True,
        'nonHTTPAndInvalidURLInertButExactIdentityRemovable':True,
        'existingNativeQuestionRetained':True, 'nativeCancelBeforeHTTP':True, 'nativeQuestionCount':len(native_questions),
        'cancelEscapeNoRetry':True, 'cookieRotationDenied':True, 'concurrentListWritePreserved':True,
        'profileRowArrayModePageRetirementNoRetry':True, 'lateFirstChallengeIgnored':True,
        'localPromptWaitBounded':True, 'lateActualCommitPreservedWithoutDOMSuccess':True,
        'unknownAndMalformedRepliesStopUntilReload':True, 'currentActorRetirementDenied':True,
        'unknownFixtureScope':'HTTP reply replacement after known actual commit; genuine datastore COMMIT uncertainty is a separate server test.',
        'playbackScope':'No playback process or policy was exercised or changed.'
    }, indent=2) + '\n')
    browser.close()
