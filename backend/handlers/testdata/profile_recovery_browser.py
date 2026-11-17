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
page_path = prefix + '/kids-settings?profileId=' + args.profile
reload_path = prefix + '/api/users/' + args.profile + '/state/reload'
pattern = '**' + reload_path

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox', '--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    errors, denied, requests, writes, previews = [], [], [], [], []

    def guard(route):
        target = urlparse(route.request.url)
        if target.scheme == 'http' and target.netloc == base.netloc:
            route.continue_()
        else:
            denied.append('non-fixture destination blocked')
            route.abort()

    context.route('**/*', guard)
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))

    def record(request):
        target = urlparse(request.url).path
        if target == reload_path:
            requests.append({'method': request.method, 'body': request.post_data,
                             'password': request.headers.get('x-account-password'), 'intent': request.headers.get('x-parent-intent')})
        elif '/api/users/' in target and request.method in ['PUT', 'POST', 'DELETE', 'PATCH']:
            writes.append(target)
        if '/api/discover/' in target or '/api/lists/custom' in target:
            previews.append(target)

    page.on('request', record)
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    button = lambda: page.locator('#reload-profile-state')
    status = lambda: page.locator('#profile-recovery-status')
    name = lambda: page.locator('#profile-name-display').inner_text()
    dialogs = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)

    def loaded(kind='normal'):
        before = len(requests)
        assert context.request.get(args.base_url + '/fixture/seed?kind=' + kind).status == 204
        page.goto(args.base_url + page_path)
        page.wait_for_function('!document.querySelector("#reload-profile-state").disabled')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert page.evaluate('typeof window.profileRecoveryFetch') == 'function'
        page.wait_for_timeout(80)
        assert len(requests) == before and dialogs().count() == 0
        return state()

    for width in [1280, 390]:
        page.set_viewport_size({'width': width, 'height': 900 if width == 1280 else 844})
        before = loaded()
        count = len(requests)
        button().click()
        page.get_by_text('Saved profile state reloaded.', exact=True).wait_for()
        after = state()
        assert len(requests) == count + 1 and after['posts'] == before['posts'] + 1
        assert after['profilesGET'] == before['profilesGET']
        assert after['physicalDigest'] == before['physicalDigest']
        assert after['cachedName'] == after['storedName'] and name() == 'Configuring: ' + after['storedName']
        assert page.locator('#movieRating').input_value() == after['movie']
        assert page.locator('#tvRating').input_value() == after['tv']
        assert dialogs().count() == 0
        assert button().evaluate('(node)=>node.getBoundingClientRect().left>=0&&node.getBoundingClientRect().right<=innerWidth')
        page.screenshot(path=str(out / ('reloaded-' + str(width) + '.png')), full_page=True)

    # Actual safe values, not an HTML fixture: the new response comes from PG.
    before = loaded('unsafe')
    button().click()
    page.get_by_text('Saved profile state reloaded.', exact=True).wait_for()
    assert name() == 'Configuring: ' + before['storedName']
    assert page.locator('#profile-name-display img').count() == 0
    assert page.evaluate('window.recoveryXSS || 0') == 0
    assert page.locator('#movieRating').input_value() == before['movie']
    assert page.locator('#tvRating').input_value() == before['tv']
    links = page.locator('#lists-container a')
    assert links.nth(0).inner_text() == before['lists'][0] and links.nth(0).get_attribute('href') is None
    assert links.nth(1).get_attribute('href') == before['lists'][1]

    # Controlled wire mutations are separate from the real successful PG case.
    malformed = [
        lambda dto: dto.pop('recovery'),
        lambda dto: dto['profile'].pop('kidsAllowedLists'),
        lambda dto: dto['profile'].update(id='other-profile'),
        lambda dto: dto['profile'].update(isKidsProfile='true'),
        lambda dto: dto['profile'].update(kidsAllowedLists=None),
        lambda dto: dto['profile'].update(kidsAllowedLists=[None]),
        lambda dto: dto['recovery'].update(usersWriteBlocked='false'),
        lambda dto: dto['profile'].update(pinHash='synthetic-field-must-not-be-accepted'),
        lambda dto: dto['profile'].pop('allowLiveTakeover'),
        lambda dto: dto['profile'].update(allowLiveTakeover=None),
        lambda dto: dto['profile'].pop('allowRecordingInterruption'),
        lambda dto: dto['profile'].update(allowRecordingInterruption='false'),
    ]
    for mutate in malformed:
        before = loaded()
        old_name = name()
        def malformed_reply(route):
            actual = route.fetch()
            assert actual.status == 200
            dto = actual.json()
            mutate(dto)
            route.fulfill(status=200, json=dto)
        page.route(pattern, malformed_reply)
        count = len(requests)
        button().click()
        page.get_by_text('Saved profile state could not be reloaded. No save was retried.', exact=True).wait_for()
        assert name() == old_name and len(requests) == count + 1 and dialogs().count() == 0
        assert state()['physicalDigest'] == before['physicalDigest']
        page.unroute(pattern, malformed_reply)

    for users_blocked, settings_pending in [(True, False), (False, True)]:
        loaded()
        def pending_reply(route):
            actual = route.fetch()
            assert actual.status == 200
            dto = actual.json()
            dto['recovery'] = {'usersWriteBlocked': users_blocked, 'settingsPendingUncertainty': settings_pending}
            route.fulfill(status=200, json=dto)
        page.route(pattern, pending_reply)
        button().click()
        expected = 'Saved profile state reloaded. Other profile changes still need recovery.' if users_blocked else 'Saved profile state reloaded. Other saved settings still need recovery.'
        page.get_by_text(expected, exact=True).wait_for()
        assert page.locator('#save-mode').is_disabled() == users_blocked
        assert page.locator('#save-rating-pair').is_disabled() == users_blocked
        assert page.locator('#add-allowed-list').is_disabled() == users_blocked
        assert not button().is_disabled()
        page.unroute(pattern, pending_reply)

    for code in ['parent_authorization_required', 'profile_recovery_changed', 'profile_recovery_unavailable']:
        loaded()
        def error_reply(route):
            route.fulfill(status=403 if code == 'parent_authorization_required' else 409 if code.endswith('changed') else 503,
                          json={'code': code, 'error': 'Synthetic controlled response', 'parentIntent': 'pmi_' + 'a' * 64})
        page.route(pattern, error_reply)
        count = len(requests)
        button().click()
        expected = 'Profile state changed. Reload again when ready.' if code.endswith('changed') else 'Saved profile state could not be reloaded. No save was retried.'
        page.get_by_text(expected, exact=True).wait_for()
        assert len(requests) == count + 1 and dialogs().count() == 0
        page.unroute(pattern, error_reply)

    # Hold an actual HTTP response AFTER the real helper/server work. Abort is
    # masked by this test-only Promise barrier; the UI's final gates still apply.
    for change in ['input', 'profile', 'form', 'input_owner', 'pagehide']:
        loaded()
        old_name = name()
        page.evaluate('''() => {
          const actual = window.profileRecoveryFetch;
          window.profileRecoveryFetch = (...args) => actual(...args).then(response => new Promise(resolve => {
            window.releaseRecoveryReply = () => resolve(response);
          }));
        }''')
        button().click()
        page.wait_for_function('typeof window.releaseRecoveryReply === "function"')
        if change == 'input':
            page.locator('#movieRating').select_option('R')
        elif change == 'profile':
            page.evaluate('profile = {...profile}')
        elif change == 'form':
            page.evaluate('document.querySelector("#kids-mode-form").replaceWith(document.querySelector("#kids-mode-form").cloneNode(true))')
        elif change == 'input_owner':
            page.evaluate('document.querySelector("#lists-section").appendChild(document.querySelector("#tvRating"))')
        else:
            page.evaluate('window.dispatchEvent(new Event("pagehide"))')
        page.evaluate('window.releaseRecoveryReply()')
        page.wait_for_timeout(80)
        assert name() == old_name
        assert status().inner_text() != 'Saved profile state reloaded.' and dialogs().count() == 0
        if change == 'input':
            assert page.locator('#movieRating').input_value() == 'R'

    # An older actual Profiles reply is held after HTTP, then a newer load and
    # explicit recovery complete first. Releasing the old reply cannot publish.
    loaded()
    page.evaluate('''() => {
      window.originalRecoveryPageFetch = window.fetch;
      window.fetch = (...args) => {
        const pending = window.originalRecoveryPageFetch(...args);
        if (!String(args[0]).includes('/api/profiles')) return pending;
        return pending.then(response => new Promise(resolve => {
          window.releaseOldProfilesReply = () => resolve(response);
        }));
      };
      void loadProfile();
    }''')
    page.wait_for_function('typeof window.releaseOldProfilesReply === "function"')
    page.evaluate('window.fetch = window.originalRecoveryPageFetch; loadProfile()')
    page.wait_for_function('!document.querySelector("#reload-profile-state").disabled')
    button().click()
    page.get_by_text('Saved profile state reloaded.', exact=True).wait_for()
    latest_name = name()
    page.evaluate('window.releaseOldProfilesReply()')
    page.wait_for_timeout(80)
    assert name() == latest_name

    # Actual revoked cookie goes through RequireAuth; no manufactured client
    # permit, password prompt or automatic session refresh is used.
    loaded()
    old_name = name()
    assert context.request.get(args.base_url + '/fixture/retire').status == 204
    count = len(requests)
    button().click()
    page.get_by_text('Saved profile state could not be reloaded. No save was retried.', exact=True).wait_for()
    assert name() == old_name and len(requests) == count + 1 and dialogs().count() == 0
    assert state()['mutationCalls'] == 0
    assert all(item['method'] == 'POST' and item['body'] in [None, ''] and item['password'] is None and item['intent'] is None for item in requests)
    assert not writes and not previews and not errors and not denied
    report = {'role': args.role, 'widths': [1280, 390], 'pageErrors': errors, 'blocked': denied,
              'requests': len(requests), 'saveRequests': writes, 'providerPreviews': previews,
              'autoRecovery': 0, 'dialogs': 0, 'malformedControlledReplies': len(malformed),
              'pendingFlagsControlledReplies': 2, 'maskedActualLateReplies': 5, 'maskedActualOldProfilesReply': 1,
              'boundary': 'Actual cookie/PG/Coordinator/assets/templates; no Main startup/login. Controlled malformed/pending errors are distinct from actual PG recovery.'}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    context.close()
    browser.close()
