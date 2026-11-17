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
password = 'synthetic-parent-password'

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox', '--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    errors, denied, requests = [], [], []

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
    page.on('request', lambda request: requests.append({'path': urlparse(request.url).path, 'method': request.method}))
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    form = lambda: page.locator('#modal-content form')
    status = lambda: form().locator('[data-profile-state-status]')
    reload_button = lambda: form().locator('[data-profile-reload]')

    def reset(kids=False):
        assert context.request.get(args.base_url + '/fixture/reset?kids=' + str(kids).lower()).status == 204

    def accounts(kids=False):
        reset(kids)
        page.goto(args.base_url + prefix + '/accounts')
        page.wait_for_function('(id) => profiles.some(profile => profile.id === id)', arg=args.profile)
        page.evaluate('(id) => showEditProfileModal(id)', args.profile)
        form().wait_for()
        assert page.evaluate('typeof window.parentProfileFlagFetch') == 'function'
        assert form().locator('[name=isKids]').is_checked() == kids
        return state()

    def save():
        form().locator('button[type=submit]').click()

    def confirm():
        dialog().wait_for()
        assert dialog().count() == 1
        dialog().locator('input[type=password]').fill(password)
        dialog().get_by_role('button', name='Confirm', exact=True).click()
        dialog().wait_for(state='detached')

    def snapshot_profile():
        return page.evaluate('(id) => {const p=profiles.find(p=>p.id===id);return {accountId:p.accountId,privacy:p.activityPrivacy,live:p.allowLiveTakeover,recording:p.allowRecordingInterruption,kids:p.isKidsProfile};}', args.profile)

    for width in [1280, 390]:
        page.set_viewport_size({'width': width, 'height': 900 if width == 1280 else 844})
        before = accounts()
        form().locator('[name=isKids]').check()
        save()
        dialog().wait_for()
        assert state()['commits'] == before['commits']
        if width == 1280:
            dialog().get_by_role('button', name='Cancel', exact=True).click()
        else:
            page.keyboard.press('Escape')
        dialog().wait_for(state='detached')
        page.wait_for_function('profileEditorAction === null')
        assert state()['retries'] == before['retries'] and not state()['kids']
        # A fresh explicit Save owns a new dialog after Cancel; no saved password.
        before = accounts()
        original = snapshot_profile()
        form().locator('[name=isKids]').check()
        save()
        dialog().wait_for()
        dialog().screenshot(path=str(out / ('accounts-confirm-' + str(width) + '.png')))
        confirm()
        page.wait_for_function('document.querySelector("#modal-overlay").style.display === "none"')
        page.wait_for_function('(id) => profiles.some(p=>p.id===id&&p.isKidsProfile)', arg=args.profile)
        after = state()
        assert after['kids'] and after['commits'] == before['commits'] + 1
        assert after['retries'] == before['retries'] + 1 and after['privacyCalls'] == before['privacyCalls'] + 1
        projected = snapshot_profile()
        assert projected['accountId'] == original['accountId'] == 'actor'
        assert projected['live'] is False and projected['recording'] is False

    # Freeze before the first cosmetic await, including same-profile modal ABA.
    for change in ['checkbox_aba', 'name', 'color', 'privacy', 'form', 'pagehide']:
        before = accounts()
        form().locator('[name=isKids]').check()
        page.evaluate('''() => {
          const actual=window.fetch;
          window.fetch=(input,options)=>{
            const pending=actual(input,options);
            if(options?.method!=='PUT'||!String(input).includes('/api/profiles?'))return pending;
            return pending.then(response=>new Promise(resolve=>{window.releaseNameReply=()=>resolve(response);}));
          };
        }''')
        save()
        page.wait_for_function('typeof window.releaseNameReply === "function"')
        if change == 'checkbox_aba':
            form().locator('[name=isKids]').uncheck()
            form().locator('[name=isKids]').check()
        elif change == 'name':
            form().locator('[name=name]').fill('New unsaved name 雪')
        elif change == 'color':
            form().locator('input[name=color]').nth(1).evaluate('(input)=>{input.checked=true;input.dispatchEvent(new Event("change",{bubbles:true}));}')
        elif change == 'privacy':
            form().locator('[name=activityPrivacy]').select_option('shared')
        elif change == 'form':
            page.evaluate('(id)=>{hideModal();showEditProfileModal(id);}', args.profile)
        else:
            page.evaluate('window.dispatchEvent(new Event("pagehide"))')
        page.evaluate('window.releaseNameReply()')
        page.wait_for_timeout(100)
        after = state()
        assert after['attempts'] == before['attempts'] and after['privacyCalls'] == before['privacyCalls']
        assert dialog().count() == 0
        if change == 'name':
            assert form().locator('[name=name]').input_value() == 'New unsaved name 雪'
        if change == 'privacy':
            assert form().locator('[name=activityPrivacy]').input_value() == 'shared'

    # Known real commit, but an HTTP error replaces its successful reply. This
    # is not a lost database COMMIT ACK. Adult recovery needs no Kids-page link.
    before = accounts(True)
    original = snapshot_profile()
    form().locator('[name=name]').fill('Draft name kept')
    form().locator('[name=activityPrivacy]').select_option('shared')
    form().locator('[name=isKids]').uncheck()
    assert context.request.get(args.base_url + '/fixture/unknown').status == 204
    save()
    confirm()
    page.get_by_text('The profile flag result is not known. Reload saved profile state before saving again. Earlier profile edits may already be saved.', exact=True).wait_for()
    committed = state()
    assert committed['kids'] is False and committed['commits'] == before['commits'] + 1
    assert committed['privacyCalls'] == before['privacyCalls']
    assert form().locator('button[type=submit]').is_disabled()
    assert context.request.get(args.base_url + '/fixture/rights').status == 204
    form().locator('[name=name]').fill('Newer unsaved name 雪')
    before_reload = state()
    reload_button().click()
    page.get_by_text('Saved profile state reloaded.', exact=True).wait_for()
    after = state()
    assert after['recoveries'] == before_reload['recoveries'] + 1 and after['profilesGET'] == before_reload['profilesGET']
    assert after['commits'] == committed['commits'] and not after['kids']
    assert form().locator('[name=name]').input_value() == 'Newer unsaved name 雪'
    assert form().locator('[name=activityPrivacy]').input_value() == 'shared'
    recovered = snapshot_profile()
    assert recovered['accountId'] == original['accountId'] and recovered['privacy'] == original['privacy']
    assert recovered['live'] is True and recovered['recording'] is False
    page.evaluate('(id)=>showLivePermissions(id)', args.profile)
    rights = page.get_by_role('dialog', name='IPTV takeover rights', exact=True)
    rights.wait_for()
    assert rights.locator('[name=allowLiveTakeover]').is_checked()
    assert not rights.locator('[name=allowRecordingInterruption]').is_checked()
    assert not rights.locator('fieldset').is_disabled()
    rights.get_by_role('button', name='Close', exact=True).click()

    # A late flag success may already be committed. It starts no Privacy write,
    # no profile GET, no modal close and never destroys later form inputs.
    before = accounts()
    form().locator('[name=isKids]').check()
    page.evaluate('''() => {const actual=window.parentProfileFlagFetch;
      window.parentProfileFlagFetch=(...args)=>actual(...args).then(response=>response.ok?new Promise(resolve=>{window.releaseFlagReply=()=>resolve(response);}):response);
    }''')
    save()
    confirm()
    page.wait_for_function('typeof window.releaseFlagReply === "function"')
    committed = state()
    form().locator('[name=name]').fill('Later input survives commit')
    page.evaluate('window.releaseFlagReply()')
    page.wait_for_timeout(100)
    assert state()['commits'] == before['commits'] + 1 and committed['kids']
    assert state()['privacyCalls'] == before['privacyCalls'] and state()['profilesGET'] == committed['profilesGET']
    assert form().locator('[name=name]').input_value() == 'Later input survives commit'
    assert reload_button().is_enabled()
    assert form().locator('button[type=submit]').is_disabled()

    # Masked recovery replies cannot replace another form, later inputs or page.
    for change in ['name', 'privacy', 'form', 'profile', 'pagehide']:
        accounts()
        page.evaluate('''() => {const actual=window.profileRecoveryFetch;
          window.profileRecoveryFetch=(...args)=>actual(...args).then(response=>new Promise(resolve=>{window.releaseSavedReply=()=>resolve(response);}));
        }''')
        reload_button().click()
        page.wait_for_function('typeof window.releaseSavedReply === "function"')
        if change == 'name':
            form().locator('[name=name]').fill('Later recovery draft')
        elif change == 'privacy':
            form().locator('[name=activityPrivacy]').select_option('shared')
        elif change == 'form':
            page.evaluate('(id)=>{hideModal();showEditProfileModal(id);}', args.profile)
        elif change == 'profile':
            page.evaluate('profiles=profiles.map(profile=>({...profile}))')
        else:
            page.evaluate('window.dispatchEvent(new Event("pagehide"))')
        page.evaluate('window.releaseSavedReply()')
        page.wait_for_timeout(100)
        assert status().inner_text() != 'Saved profile state reloaded.'
        if change == 'name':
            assert form().locator('[name=name]').input_value() == 'Later recovery draft'

    # Settings has an existing dormant renderer (not exposed by SettingsSchema).
    # Mount that actual renderer and bind its real controls without claiming a
    # new production navigation entry. Accounts above is naturally visible.
    def settings():
        reset()
        page.goto(args.base_url + prefix + '/settings')
        page.wait_for_load_state('networkidle')
        page.wait_for_function('(id)=>profilesData.some(profile=>profile.id===id)', arg=args.profile)
        page.evaluate('''() => {
          document.getElementById('settingsCategoryWorkspace').hidden=false;
          document.getElementById('settingsHome').hidden=true;
          const host=document.getElementById('settingsContainer');
          host.innerHTML=renderProfilesSection();bindSettingsFlagRows(host);
          const draft=document.createElement('input');draft.id='unrelated-live-draft';draft.value='keep this draft';host.append(draft);
        }''')
        toggle().wait_for()
        return state()

    toggle = lambda: page.locator('[data-kids-flag="' + str(page.evaluate('(id)=>profilesData.findIndex(p=>p.id===id)', args.profile)) + '"]')
    settings_row = lambda: toggle().locator('xpath=ancestor::div[contains(concat(" ",normalize-space(@class)," ")," array-item ")][1]')
    for width in [1280, 390]:
        page.set_viewport_size({'width': width, 'height': 900 if width == 1280 else 844})
        before = settings()
        toggle().check()
        confirm()
        settings_row().get_by_text('Kids profile enabled', exact=True).wait_for()
        assert state()['commits'] == before['commits'] + 1
        assert page.locator('#unrelated-live-draft').input_value() == 'keep this draft'
        settings_row().locator('[data-kids-flag-reload]').click()
        settings_row().get_by_text('Saved profile state reloaded.', exact=True).wait_for()
        assert page.locator('#unrelated-live-draft').input_value() == 'keep this draft'
        page.screenshot(path=str(out / ('settings-mounted-' + str(width) + '.png')), full_page=True)

    before = settings()
    assert context.request.get(args.base_url + '/fixture/unknown').status == 204
    toggle().check()
    confirm()
    settings_row().get_by_text('The profile flag result is not known. Reload saved profile state before saving again.', exact=True).wait_for()
    assert state()['commits'] == before['commits'] + 1 and toggle().is_disabled()
    settings_row().locator('[data-kids-flag-reload]').click()
    settings_row().get_by_text('Saved profile state reloaded.', exact=True).wait_for()
    assert toggle().is_checked() and toggle().is_enabled()

    for change in ['scope_aba', 'row', 'profile', 'input']:
        before = settings()
        toggle().check()
        dialog().wait_for()
        if change == 'scope_aba':
            page.evaluate('const old=selectedClientId;selectedClientId="synthetic-other";renderSettings();selectedClientId=old;renderSettings();')
        elif change == 'row':
            page.evaluate('const host=document.getElementById("settingsContainer");retireSettingsFlagActions();host.innerHTML=renderProfilesSection();bindSettingsFlagRows(host);')
        elif change == 'profile':
            page.evaluate('profilesData=profilesData.map(profile=>({...profile}))')
        else:
            # Pending actions intentionally disable user interaction. Exercise
            # external DOM invalidation without pretending this is a user click.
            assert toggle().is_disabled()
            toggle().evaluate('(control) => {control.checked=false;control.dispatchEvent(new Event("change",{bubbles:true}));}')
        # Profile object replacement is caught by the final predicate even if
        # it did not fire a DOM event and leave the dialog immediately.
        if dialog().count():
            confirm()
        page.wait_for_timeout(100)
        assert state()['retries'] == before['retries'] and state()['commits'] == before['commits']

    # Actual response work can finish before a newer row/input replaces it.
    before = settings()
    page.evaluate('''() => {const actual=window.parentProfileFlagFetch;
      window.parentProfileFlagFetch=(...args)=>actual(...args).then(response=>response.ok?new Promise(resolve=>{window.releaseSettingsFlag=()=>resolve(response);}):response);
    }''')
    toggle().check()
    confirm()
    page.wait_for_function('typeof window.releaseSettingsFlag === "function"')
    page.evaluate('const host=document.getElementById("settingsContainer");retireSettingsFlagActions();host.innerHTML=renderProfilesSection();bindSettingsFlagRows(host);')
    page.evaluate('window.releaseSettingsFlag()')
    page.wait_for_timeout(100)
    assert state()['commits'] == before['commits'] + 1
    assert page.evaluate('(id)=>profilesData.find(p=>p.id===id).isKidsProfile', args.profile) is False
    assert not toggle().is_checked()

    for change in ['name', 'checkbox_aba', 'form', 'pagehide']:
        accounts()
        original = snapshot_profile()
        page.evaluate('''() => {const actual=window.profileRecoveryFetch;
          window.profileRecoveryFetch=(...args)=>actual(...args).then(response=>new Promise(resolve=>{window.releaseClientRecovery=()=>resolve(response);}));
        }''')
        reload_button().click()
        page.wait_for_function('typeof window.releaseClientRecovery === "function"')
        if change == 'name':
            form().locator('[name=name]').fill('Later recovery draft')
        elif change == 'checkbox_aba':
            form().locator('[name=isKids]').check()
            form().locator('[name=isKids]').uncheck()
        elif change == 'form':
            page.evaluate('(id)=>{hideModal();showEditProfileModal(id);}', args.profile)
        else:
            page.evaluate('window.dispatchEvent(new Event("pagehide"))')
        page.evaluate('window.releaseClientRecovery()')
        page.wait_for_timeout(100)
        assert snapshot_profile() == original
        assert form().locator('[data-profile-state-status]').inner_text() != 'Saved profile state reloaded.'
        if change == 'name':
            assert form().locator('[name=name]').input_value() == 'Later recovery draft'

    # Separate actual json() barriers: HTTP and real commit/read have already
    # completed, but the caller has entered its JSON await before retirement.
    before = accounts()
    form().locator('[name=isKids]').check()
    page.evaluate('''() => {const actual=window.parentProfileFlagFetch;
      window.parentProfileFlagFetch=(...args)=>actual(...args).then(response=>{
        if(!response.ok)return response;
        const json=response.json.bind(response);
        response.json=()=>json().then(value=>new Promise(resolve=>{window.releaseFlagJSON=()=>resolve(value);}));
        return response;
      });
    }''')
    save()
    confirm()
    page.wait_for_function('typeof window.releaseFlagJSON === "function"')
    form().locator('[name=name]').fill('JSON-stage newer input')
    page.evaluate('window.releaseFlagJSON()')
    page.wait_for_timeout(100)
    assert state()['commits'] == before['commits'] + 1
    assert state()['privacyCalls'] == before['privacyCalls']
    assert snapshot_profile()['kids'] is False
    assert form().locator('[name=name]').input_value() == 'JSON-stage newer input'

    before = settings()
    page.evaluate('''() => {const actual=window.profileRecoveryFetch;
      window.profileRecoveryFetch=(...args)=>actual(...args).then(response=>{
        const json=response.json.bind(response);
        response.json=()=>json().then(value=>new Promise(resolve=>{window.releaseSettingsJSON=()=>resolve(value);}));
        return response;
      });
    }''')
    settings_row().locator('[data-kids-flag-reload]').click()
    page.wait_for_function('typeof window.releaseSettingsJSON === "function"')
    page.locator('#unrelated-live-draft').fill('Newer unrelated draft')
    page.evaluate('const host=document.getElementById("settingsContainer");retireSettingsFlagActions();host.innerHTML=renderProfilesSection();bindSettingsFlagRows(host);const input=document.createElement("input");input.id="unrelated-live-draft";input.value="Newer unrelated draft";host.append(input);')
    page.evaluate('window.releaseSettingsJSON()')
    page.wait_for_timeout(100)
    assert state()['recoveries'] == before['recoveries'] + 1
    assert state()['commits'] == before['commits']
    assert settings_row().locator('[data-kids-flag-status]').inner_text() == ''
    assert page.locator('#unrelated-live-draft').input_value() == 'Newer unrelated draft'

    # Foreign Settings uncertainty is only a notice; Users global stop blocks
    # Save. Both are controlled response mutations after actual recovery.
    for users_blocked, settings_pending in [(False, True), (True, False)]:
        accounts()
        pattern = '**' + prefix + '/api/users/' + args.profile + '/state/reload'
        def pending_reply(route):
            actual = route.fetch()
            assert actual.status == 200
            value = actual.json()
            value['recovery'] = {'usersWriteBlocked': users_blocked, 'settingsPendingUncertainty': settings_pending}
            route.fulfill(status=200, json=value)
        page.route(pattern, pending_reply)
        reload_button().click()
        expected = 'Saved profile state reloaded. Other profile changes still need recovery.' if users_blocked else 'Saved profile state reloaded. Other saved settings still need recovery.'
        page.get_by_text(expected, exact=True).wait_for()
        assert form().locator('button[type=submit]').is_disabled() == users_blocked
        page.unroute(pattern, pending_reply)

    accounts()
    old = snapshot_profile()
    assert context.request.get(args.base_url + '/fixture/retire').status == 204
    before = state()
    reload_button().click()
    page.get_by_text('Saved profile state could not be reloaded. No save was retried.', exact=True).wait_for()
    assert snapshot_profile() == old and state()['commits'] == before['commits'] and dialog().count() == 0
    assert not errors and not denied
    report = {'role': args.role, 'widths': [1280, 390], 'pageErrors': errors, 'blocked': denied,
              'naturallyVisibleAccounts': True, 'settingsBoundary': 'Real Settings page; explicit synthetic mount of existing dormant renderProfilesSection and actual row binder; no navigation activation.',
              'maskedActualLateCosmeticCases': 6, 'maskedActualLateFlagSuccess': 1, 'maskedActualLateRecoveryCases': 4, 'maskedActualLateSettingsFlagSuccess': 1, 'maskedActualJSONStages': ['accounts-flag', 'settings-recovery'],
              'knownCommitHTTPUnavailable': 2, 'lostDatabaseCommitACKClaim': False, 'controlledPendingReplies': 2,
              'boundary': 'Actual cookie/PG/confirmation/Recovery/templates/assets and literal-loopback HTTP, not Main startup/login or new PIN/playback policy.'}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    context.close()
    browser.close()
