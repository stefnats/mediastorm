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
mode_path = prefix + '/api/users/' + args.profile + '/kids/lists'
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
    button = lambda: page.locator('#add-allowed-list')
    input_field = lambda: page.locator('#new-list-url')

    def loaded():
        assert context.request.get(args.base_url + '/fixture/seed').status == 204
        page.goto(args.base_url + path)
        button().wait_for()
        page.wait_for_function('!document.querySelector("#add-allowed-list").disabled')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert page.evaluate('typeof window.parentListAdditionFetch') == 'function'
        assert page.evaluate('typeof window.parentListReplacementFetch') == 'undefined'

    def start(value=None):
        before = state()
        identity = before['candidate'] if value is None else value
        input_field().fill(identity)
        button().click()
        dialog().wait_for()
        assert dialog().count() == 1 and state()['writes'] == before['writes']
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

    for width in [1280, 390]:
        page.set_viewport_size({'width':width,'height':900 if width==1280 else 844})
        loaded()
        before, _ = start()
        if width == 1280:
            dialog().get_by_role('button',name='Cancel',exact=True).click()
        else:
            page.keyboard.press('Escape')
        dialog().wait_for(state='detached')
        no_retry(before)
        page.wait_for_function('!document.querySelector("#add-allowed-list").disabled')
        before, identity = start()
        assert dialog().evaluate('(element)=>element.getBoundingClientRect().left>=0&&element.getBoundingClientRect().right<=innerWidth')
        dialog().screenshot(path=str(out / ('confirm-' + str(width) + '.png')))
        confirm()
        page.get_by_text('List added',exact=True).wait_for()
        after = state()
        assert after['lists'] == before['lists'] + [identity] and after['writes'] == before['writes'] + 1
        assert after['kids'] is True and after['mode'] == before['mode'] == 'content_list'
        assert input_field().input_value() == '' and password not in page.content()
        assert not page.evaluate('Boolean(window.__listRemovalInjected)')
        # Duplicate is freshly confirmed but must be a real target-row no-op.
        before, _ = start(identity)
        confirm()
        page.wait_for_function('document.querySelector("#new-list-url").value === "" && !document.querySelector("#add-allowed-list").disabled')
        after = state()
        assert after['lists'] == before['lists'] and after['writes'] == before['writes']
        assert after['updatedAt'] == before['updatedAt'] and after['retries'] == before['retries'] + 1

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/rotate').status == 204
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.',exact=True).wait_for()
    assert state()['writes'] == before['writes']

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/mutate-list').status == 204
    changed = state()['lists']
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.',exact=True).wait_for()
    assert state()['writes'] == before['writes'] and state()['lists'] == changed

    loaded()
    before, _ = start()
    replacement = 'https://mdblist.com/lists/synthetic/new-input'
    page.evaluate("value=>{const field=document.querySelector('#new-list-url');field.value=value;field.dispatchEvent(new Event('input',{bubbles:true}))}",replacement)
    dialog().wait_for(state='detached')
    no_retry(before)
    assert input_field().input_value() == replacement

    loaded()
    before, _ = start()
    page.evaluate("history.replaceState({},'', '?profileId=different-profile')")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("const original=document.querySelector('#new-list-url');original.replaceWith(original.cloneNode(true))")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("profile.kidsAllowedLists.push('synthetic-local-array-change')")
    confirm()
    no_retry(before)

    loaded()
    before, _ = start()
    page.evaluate("const radio=document.querySelector('input[name=kidsMode][value=rating]');radio.checked=true;radio.dispatchEvent(new Event('change',{bubbles:true}))")
    dialog().wait_for(state='detached')
    no_retry(before)

    # Keep canceled challenges within the actual shared eight-per-actor budget.
    loaded()
    before, _ = start()
    dialog().wait_for(state='detached',timeout=33000)
    no_retry(before)

    loaded()
    before, _ = start()
    page.goto(args.base_url + '/fixture/away')
    no_retry(before)

    loaded()
    held = []

    def hold_first(route):
        held.append((route,route.fetch()))

    page.route(pattern,hold_first)
    before = state()
    input_field().fill(before['candidate'])
    button().click()
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held
    page.goto(args.base_url + '/fixture/away')
    route,response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass
    page.unroute(pattern,hold_first)
    assert dialog().count() == 0
    no_retry(before)

    loaded()
    held = []

    def hold_success(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            held.append((route,response))
        else:
            route.fulfill(response=response)

    page.route(pattern,hold_success)
    before,identity = start()
    confirm()
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held and state()['writes'] == before['writes'] + 1
    input_field().fill(replacement)
    route,response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass
    page.unroute(pattern,hold_success)
    page.wait_for_timeout(100)
    assert state()['lists'] == before['lists'] + [identity]
    assert input_field().input_value() == replacement
    assert page.get_by_text('List added',exact=True).count() == 0

    loaded()

    def unknown_after_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=503,content_type='application/json',body=json.dumps({'code':'parent_confirmation_outcome_unknown','error':'PRIVATE_REPLY_CANARY'}))
        else:
            route.fulfill(response=response)

    page.route(pattern,unknown_after_commit)
    before,identity = start()
    confirm()
    page.get_by_text('The result is not known. Reload settings before trying again.',exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1 and state()['lists'] == before['lists'] + [identity]
    attempts = state()['attempts']
    input_field().fill(replacement)
    assert button().is_disabled() and state()['attempts'] == attempts and 'PRIVATE_REPLY_CANARY' not in page.content()
    page.unroute(pattern,unknown_after_commit)

    loaded()

    def malformed_after_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=200,content_type='application/json',body=json.dumps({'id':args.profile,'kidsAllowedLists':None}))
        else:
            route.fulfill(response=response)

    page.route(pattern,malformed_after_commit)
    before, _ = start()
    confirm()
    page.get_by_text('The result could not be confirmed. Reload settings before trying again.',exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1 and button().is_disabled()
    page.unroute(pattern,malformed_after_commit)

    loaded()
    before, _ = start()
    assert context.request.get(args.base_url + '/fixture/retire-actor').status == 204
    confirm()
    page.wait_for_timeout(100)
    assert state()['writes'] == before['writes']
    assert page.get_by_text('List added',exact=True).count() == 0
    assert not errors and not denied
    (out / 'result.json').write_text(json.dumps({
        'fixture':'actual-cookie-template-asset-own-schema-pg-parent-list-Add', 'role':args.role, 'widths':[1280,390],
        'pageErrors':errors,'blockedPageRequests':denied,'freshMasterProof':args.role=='master',
        'quoteUnicodeLiteralPercentPlusAppend':True,'duplicateTrueNoopNoUpdatedAt':True,
        'onePasswordDialogPerExplicitAdd':True,'cancelEscapeNoRetry':True,'cookieRotationDenied':True,
        'originalInputFormArrayProfileModePageBound':True,'concurrentArrayWritePreserved':True,
        'lateFirstChallengeIgnored':True,'localPromptWaitBounded':True,
        'lateActualCommitDoesNotClearNewInputOrPublishSuccess':True,'unknownAndMalformedRequireReload':True,
        'noReplacementWebCallerAdded':True,'actorRetirementDenied':True,
        'unknownFixtureScope':'HTTP reply replaced after known actual commit; real database COMMIT uncertainty is separately tested by server tests.',
        'playbackScope':'No playback process or policy was exercised or changed.'
    },indent=2) + '\n')
    browser.close()
