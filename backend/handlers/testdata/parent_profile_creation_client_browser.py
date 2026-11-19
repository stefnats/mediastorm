"""Exercise real rendered creation consumers on the isolated Cookie/PG fixture.

The deliberately masked HTTP response follows a known successful handler reply.
It is not a PostgreSQL COMMIT-ACK fault, startup/login or device acceptance.
"""
import argparse
import ipaddress
import json
import re
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument('--base-url', required=True)
parser.add_argument('--chromium', required=True)
parser.add_argument('--artifacts', required=True)
parser.add_argument('--role', choices=['master', 'regular'], required=True)
args = parser.parse_args()
base = urlparse(args.base_url)
assert base.scheme == 'http' and base.hostname == '127.0.0.1' and ipaddress.ip_address(base.hostname).is_loopback
assert base.port and base.path in ['', '/']
assert not base.username and not base.password and not base.query and not base.fragment
out = Path(args.artifacts)
out.mkdir(parents=True, exist_ok=True)
prefix = '/admin' if args.role == 'master' else '/account'
create_path = prefix + '/api/profiles'
read_prefix = create_path + '/creation-operations/'

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=[
        '--no-sandbox', '--disable-background-networking', '--disable-component-update',
        '--disable-sync', '--no-first-run',
    ])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    denied, errors, requests, retained_before_retry, checkpoints = [], [], [], [], []
    read_responses, read_snapshots = [], []

    def persist_read_diagnostics():
        (out / 'read-diagnostics.json').write_text(json.dumps({
            'role': args.role, 'responses': read_responses, 'snapshots': read_snapshots,
            'boundary': 'Bounded passive network DTO shape and panel state only; no credentials, bodies, profile values or header values.',
        }, indent=2) + '\n')

    def fixture_target(url, expected_path=None):
        target = urlparse(url)
        return (target.scheme == 'http' and target.hostname == '127.0.0.1' and
                target.netloc == base.netloc and not target.username and not target.password and
                not target.fragment and (expected_path is None or
                target.path == expected_path and not target.query and not target.params))

    def guard(route):
        if fixture_target(route.request.url):
            route.continue_()
        else:
            denied.append('non-fixture destination blocked')
            route.abort()

    def require_create_destination(route):
        # Page routes override the context guard. Every interception must repeat
        # both the literal fixture origin and exact Create route boundary.
        if fixture_target(route.request.url, create_path):
            return True
        denied.append('non-fixture creation destination blocked')
        route.abort()
        return False

    context.route('**/*', guard)
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))

    def record(request):
        path = urlparse(request.url).path
        if path == create_path or path.startswith(read_prefix):
            # Record only presence. Never serialize a password, Cookie or body.
            requests.append({'path': path, 'method': request.method,
                             'passwordPresent': 'x-account-password' in request.headers,
                             'intentPresent': 'x-parent-intent' in request.headers,
                             'operationPresent': 'x-profile-creation-operation' in request.headers})

    page.on('request', record)

    def record_read_response(response):
        target = urlparse(response.url)
        if (len(read_responses) >= 16 or not fixture_target(response.url) or target.query or
                response.request.method != 'POST' or
                not re.fullmatch(re.escape(read_prefix) + r'pcr_[0-9a-f]{64}/read', target.path)):
            return
        entry = {'status': response.status}
        read_responses.append(entry)
        try:
            # Playwright reads the network response independently. This never
            # calls the page's Response.json or releases its forced JSON barrier.
            raw = response.body()
            if len(raw) > 65536:
                entry['bodyShape'] = 'over_diagnostic_limit'
            else:
                value = json.loads(raw)
                entry['jsonType'] = type(value).__name__
                if isinstance(value, dict):
                    entry['keys'] = sorted(str(key)[:64] for key in value)[:32]
                    entry['fieldTypes'] = {key: type(value[key]).__name__ for key in [
                        'outcome', 'profile', 'usersWriteBlocked', 'settingsPendingUncertainty'] if key in value}
                    outcome = value.get('outcome')
                    entry['outcome'] = outcome if outcome in [
                        'created', 'not_created', 'pending', 'changed_or_replaced', 'unqualified_or_missing'] else 'other_or_missing'
                    entry['flags'] = {key: value[key] for key in [
                        'usersWriteBlocked', 'settingsPendingUncertainty'] if isinstance(value.get(key), bool)}
                    profile = value.get('profile')
                    if isinstance(profile, dict):
                        keys = sorted(profile)[:32]
                        entry['profileKeys'] = [str(key)[:64] for key in keys]
                        entry['profileFieldTypes'] = {str(key)[:64]: type(profile[key]).__name__ for key in keys}
        except Exception as error:
            entry['observationErrorType'] = type(error).__name__
        persist_read_diagnostics()

    page.on('response', record_read_response)
    # Forced JSON completion is a separate browser boundary from masked wire
    # delivery. The real Response JSON is parsed first and then withheld locally.
    page.add_init_script("""(() => {
      const original = Response.prototype.json;
      Response.prototype.json = async function(...args) {
        const value = await original.apply(this, args);
        const path = new URL(this.url, location.href).pathname;
        const mode = window.creationFixtureJSONMode;
        if (this.status === 200 && ((mode === 'create' && path.endsWith('/api/profiles')) ||
            (mode === 'read' && path.includes('/api/profiles/creation-operations/')))) {
          window.creationFixtureJSONMode = null;
          window.creationFixtureJSONWaiting = true;
          await new Promise(resolve => { window.creationFixtureReleaseJSON = resolve; });
          window.creationFixtureJSONWaiting = false;
        }
        return value;
      };
    })();""")
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    stored = lambda: page.evaluate("""basePath => window.createProfileCreationClient({
      actorAccountID:'actor',basePath}).list()""", prefix)
    row = lambda ref: page.locator('[data-creation-operation="' + ref + '"]')

    def wait_idle():
        page.wait_for_function("""() => profileCreationAction === null &&
          document.querySelector('dialog[aria-label="Confirm account password"]') === null""")

    def snapshot_read(stage, kind):
        entry = {'stage': stage, 'kind': kind}
        try:
            entry['panel'] = page.evaluate("""() => {
              const panel=document.getElementById('profile-creation-results');
              const read=profileCreationReadAction;
              return {statusText:(panel?.querySelector('[data-profile-creation-status]')?.textContent || '').slice(0,1024),
                panelConnected:!!panel?.isConnected,panelHidden:panel?.hidden,
                readActive:!!read,readCurrent:read ? !!read.isCurrent() : null,
                readAborted:read ? read.controller.signal.aborted : null,
                writeActive:!!profileCreationAction,pageRetired:profileCreationPageRetired,
                generation:profileCreationGeneration,usersWriteBlocked:profileCreationUsersWriteBlocked,
                uncertainCount:profileCreationUncertainOperations.size,retainedCount:profileCreationRetainedOperations.size,
                outcomes:Array.from(profileCreationResults.values()).slice(0,8).map(value=>({
                  outcome:['created','not_created','pending','changed_or_replaced','unqualified_or_missing'].includes(value.outcome) ? value.outcome : 'other_or_missing',
                  usersWriteBlocked:typeof value.usersWriteBlocked==='boolean' ? value.usersWriteBlocked : null,
                  settingsPendingUncertainty:typeof value.settingsPendingUncertainty==='boolean' ? value.settingsPendingUncertainty : null})),
                jsonWaiting:window.creationFixtureJSONWaiting===true};
            }""")
        except Exception as error:
            entry['snapshotErrorType'] = type(error).__name__
        if len(read_snapshots) < 8:
            read_snapshots.append(entry)
        persist_read_diagnostics()

    def wait_created_result(ref, kind):
        snapshot_read('after_explicit_read_click', kind)
        try:
            page.wait_for_function("""ref => document.querySelector('[data-creation-operation="'+ref+'"]')?.textContent.toLowerCase().includes('created')""", arg=ref)
        except PlaywrightTimeoutError:
            snapshot_read('created_result_timeout', kind)
            try:
                page.screenshot(path=str(out / ('read-timeout-' + kind + '.png')), full_page=True, timeout=5000)
            except Exception as error:
                if len(read_snapshots) < 8:
                    read_snapshots.append({'stage': 'failure_screenshot', 'errorType': type(error).__name__})
            persist_read_diagnostics()
            raise

    def loaded(kind, clear=True):
        path = prefix + ('/onboarding' if kind == 'onboarding' else '/accounts')
        before = state()['reads']
        page.goto(args.base_url + path)
        page.wait_for_function("typeof window.createProfileCreationClient === 'function'")
        if clear:
            page.evaluate('sessionStorage.clear()')  # Explicit fixture isolation.
            page.reload()
        if kind == 'onboarding':
            page.wait_for_function("document.querySelector('#profileAccountId').options.length > 0")
            page.wait_for_function("getHighestUnlocked(getRawStepDone()) >= 4")
            page.evaluate('goToStep(4)')
        else:
            page.wait_for_function("typeof showCreateProfileModal === 'function' && accounts.length > 0")
            page.evaluate("showCreateProfileModal('actor')")
        form = page.locator('[data-profile-create]')
        form.wait_for(state='visible')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert state()['reads'] == before, 'Page load must not automatically read a creation result'
        return form

    def fill(kind, label, optional=True):
        form = page.locator('[data-profile-create]')
        if kind == 'onboarding':
            page.locator('#profileAccountId').select_option('actor')
            page.locator('#profileName').fill(label)
            kids = page.locator('#profileKids')
            if not kids.is_checked():
                page.locator('label.toggle').filter(has=kids).click()
            assert kids.is_checked()
            if optional:
                page.locator('#profilePin').fill('2468')
        else:
            form.locator('input[name=name]').fill(label)
            form.locator('input[name=isKids]').check()
            if optional:
                form.locator('input[name=iconUrl]').fill(args.base_url + '/fixture/icon.png')
        assert page.locator('[data-creation-followup-notice]').is_visible()
        return form

    def name_input(kind):
        return page.locator('#profileName' if kind == 'onboarding' else '[data-profile-create] input[name=name]')

    geometry_checks = []

    def assert_onboarding_geometry(kind, stage):
        if kind != 'onboarding':
            return
        required = ['.onboarding-progress-card', 'section[data-panel="4"].active',
                    'form[data-profile-create]', '#profileAccountId', '#profileName', '#profilePin',
                    '#profileColor', 'label.toggle:has(#profileKids)',
                    'form[data-profile-create] button[type=submit]']
        geometry = page.evaluate("""selectors => {
          const required=selectors.map(selector=>({selector,element:document.querySelector(selector)}));
          const optional=Array.from(document.querySelectorAll(
            '[data-creation-read],[data-creation-forget],dialog[aria-label="Confirm account password"],dialog[aria-label="Confirm account password"] input[type=password]'
          )).map(element=>({selector:element.tagName,element}));
          const controls=required.concat(optional).map(({selector,element},index)=>{
            if(!element)return {selector,required:index<required.length,visible:false};
            const rect=element.getBoundingClientRect(),style=getComputedStyle(element);
            return {selector,required:index<required.length,
              visible:style.display!=='none'&&style.visibility!=='hidden'&&rect.width>0&&rect.height>0,
              left:rect.left,right:rect.right,width:rect.width};
          });
          return {viewport:window.innerWidth,clientWidth:document.documentElement.clientWidth,
            documentScrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,controls};
        }""", required)
        geometry_checks.append({'stage': stage, **geometry})
        (out / 'layout-geometry.json').write_text(json.dumps(geometry_checks, indent=2) + '\n')
        assert geometry['clientWidth'] == geometry['viewport'], 'Unexpected layout viewport'
        assert geometry['documentScrollWidth'] <= geometry['viewport'], 'Onboarding document overflows horizontally'
        assert geometry['bodyScrollWidth'] <= geometry['viewport'], 'Onboarding body overflows horizontally'
        for control in geometry['controls']:
            if control['required']:
                assert control['visible'], control['selector'] + ' unexpectedly hidden'
            if control['visible']:
                assert control['left'] >= -0.5 and control['right'] <= geometry['viewport'] + 0.5, control['selector'] + ' is clipped horizontally'

    def confirm():
        dialog().wait_for(state='visible')
        dialog().locator('input[type=password]').fill('synthetic-web-parent')
        dialog().get_by_role('button', name='Confirm', exact=True).click()

    def check_no_followups(before, after):
        for key in ['icons', 'pins', 'otherWrites']:
            assert after[key] == before[key], key + ' unexpectedly changed'

    def check_compound(before, after):
        assert after['posts'] == before['posts'] + 2
        assert after['retries'] == before['retries'] + 1
        assert after['commits'] == before['commits'] + 1
        for key in ['users', 'settings', 'createdResults']:
            assert after[key] == before[key] + 1, 'Actual compound ' + key + ' count'
        check_no_followups(before, after)

    def verify_retained(route):
        if not require_create_destination(route):
            return
        request = route.request
        if request.method == 'POST' and request.headers.get('x-parent-intent'):
            ref = request.headers.get('x-profile-creation-operation')
            assert re.fullmatch(r'pcr_[0-9a-f]{64}', ref or '')
            assert re.fullmatch(r'pcc_[0-9a-f]{64}', request.headers['x-parent-intent'])
            entries = stored()
            assert any(entry['ref'] == ref and entry['ownerID'] == 'actor' for entry in entries)
            assert all(set(entry) == {'ref', 'ownerID', 'createdAt'} for entry in entries)
            raw = page.evaluate('JSON.stringify(Object.entries(sessionStorage))')
            for secret in ['synthetic-web-parent', 'pcc_', '/fixture/icon.png']:
                assert secret not in raw, 'Secret or optional value persisted'
            retained_before_retry.append(True)
        route.continue_()

    page.route('**' + create_path, verify_retained)
    kinds = ['accounts'] + (['onboarding'] if args.role == 'master' else [])
    for kind in kinds:
        for width, height in [(1280, 900), (390, 844)]:
            page.set_viewport_size({'width': width, 'height': height})
            loaded(kind)
            assert_onboarding_geometry(kind, 'idle-' + str(width))
            if kind == 'onboarding' and width == 390:
                # Probe a narrower idle form without changing its DOM or values.
                try:
                    page.set_viewport_size({'width': 360, 'height': 800})
                    assert_onboarding_geometry(kind, 'idle-360')
                finally:
                    page.set_viewport_size({'width': width, 'height': height})
                assert_onboarding_geometry(kind, 'idle-390-restored')
            form = fill(kind, 'Synthetic direct ' + kind + ' ' + str(width))
            before = state()
            form.locator('button[type=submit]').click()
            dialog().wait_for(state='visible')
            assert len(stored()) == 1
            assert state()['commits'] == before['commits']
            assert_onboarding_geometry(kind, 'confirmation-' + str(width))
            page.screenshot(path=str(out / ('confirmation-' + kind + '-' + str(width) + '.png')), full_page=True)
            confirm()
            page.wait_for_function("""basePath => window.createProfileCreationClient({
              actorAccountID:'actor',basePath}).list().length === 0""", arg=prefix)
            after = state()
            check_compound(before, after)
            assert dialog().count() == 0
            checkpoints.append({'case': 'direct-' + kind + '-' + str(width), 'created': after['createdResults']})

        # Actual successful handler response masked by the Go fixture. The
        # locator survives reload; a new explicit read never continues icon/PIN.
        loaded(kind)
        form = fill(kind, 'Synthetic masked ' + kind)
        before = state()
        assert context.request.get(args.base_url + '/fixture/mask-next').status == 204
        form.locator('button[type=submit]').click()
        confirm()
        page.wait_for_function("document.querySelector('[data-creation-read]') !== null")
        ref = stored()[0]['ref']
        page.wait_for_function("""ref => !document.querySelector('[data-creation-operation="'+ref+'"] [data-creation-read]').disabled""", arg=ref)
        check_compound(before, state())
        loaded(kind, clear=False)
        draft = 'Newer draft survives explicit read ' + kind
        fill(kind, draft)
        before_read = state()
        row(ref).locator('[data-creation-read]').click()
        wait_created_result(ref, kind)
        assert stored() == []
        after_read = state()
        assert after_read['reads'] == before_read['reads'] + 1
        for key in ['posts', 'retries', 'commits', 'users', 'settings', 'createdResults', 'profilesGET']:
            assert after_read[key] == before_read[key], 'Read changed ' + key
        check_no_followups(before_read, after_read)
        assert name_input(kind).input_value() == draft
        assert page.locator('[data-profile-create]').is_visible()
        assert dialog().count() == 0
        assert_onboarding_geometry(kind, 'recovered-390')
        page.screenshot(path=str(out / ('recovered-' + kind + '-390.png')), full_page=True)

        # Actual challenge cancellation, input ABA and duplicate submission:
        # no password-bearing retry or durable create is allowed.
        loaded(kind)
        form = fill(kind, 'Synthetic canceled ' + kind)
        before = state()
        form.locator('button[type=submit]').click()
        dialog().wait_for(state='visible')
        page.evaluate("document.querySelector('[data-profile-create]').requestSubmit()")
        assert state()['posts'] == before['posts'] + 1
        prepared_ref = stored()[0]['ref']
        dialog().get_by_role('button', name='Cancel', exact=True).click()
        wait_idle()
        assert state()['commits'] == before['commits'] and state()['retries'] == before['retries']
        # Acknowledgment keeps its prepared locator even when no retry was
        # dispatched. Cancel never supplies a not-created proof or discards it.
        assert len(stored()) == 1 and stored()[0]['ref'] == prepared_ref
        check_no_followups(before, state())

        loaded(kind)
        form = fill(kind, 'Synthetic ABA ' + kind)
        before = state()
        form.locator('button[type=submit]').click()
        dialog().wait_for(state='visible')
        # A modal dialog blocks ordinary clicks in the underlying page. This
        # controlled DOM event ABA tests an asynchronous form update, rather
        # than pretending a user can click through the native modal layer.
        name_input(kind).evaluate("""input => {
          const original=input.value;
          for (const value of [original+' edited',original]) {
            input.value=value;input.dispatchEvent(new Event('input',{bubbles:true}));
          }
        }""")
        page.wait_for_function("document.querySelector('dialog[aria-label=\"Confirm account password\"]') === null")
        assert state()['retries'] == before['retries'] and state()['commits'] == before['commits']

        # Separate forced late JSON after real compound success. Input retirement
        # cannot reset the replacement draft or launch captured optional writes.
        loaded(kind)
        form = fill(kind, 'Synthetic late JSON ' + kind)
        before = state()
        page.evaluate("window.creationFixtureJSONMode='create'")
        form.locator('button[type=submit]').click()
        confirm()
        page.wait_for_function('window.creationFixtureJSONWaiting === true')
        newer = 'Draft after real commit ' + kind
        name_input(kind).fill(newer)
        page.evaluate('window.creationFixtureReleaseJSON()')
        page.wait_for_function('window.creationFixtureJSONWaiting === false')
        check_compound(before, state())
        assert name_input(kind).input_value() == newer
        assert page.locator('[data-profile-create]').is_visible()
        assert len(stored()) == 1
        check_no_followups(before, state())

        # A fresh read has its own lifetime; late JSON cannot adopt/resolve when
        # its result panel or page lifetime has been retired.
        ref = stored()[0]['ref']
        loaded(kind, clear=False)
        page.evaluate("window.creationFixtureJSONMode='read'")
        row(ref).locator('[data-creation-read]').click()
        page.wait_for_function('window.creationFixtureJSONWaiting === true')
        if kind == 'accounts':
            page.evaluate("showCreateProfileModal('actor')")
        else:
            page.evaluate('goToStep(3)')
        page.evaluate('window.creationFixtureReleaseJSON()')
        page.wait_for_function('window.creationFixtureJSONWaiting === false')
        assert any(entry['ref'] == ref for entry in stored())
        assert dialog().count() == 0
        check_no_followups(before, state())

        # Hold the actual known-success wire reply until the real page lifetime
        # changes. This is distinct from the parsed-JSON barrier above. Each
        # request still goes through Main and commits once before the edit.
        def late_wire_for(original_page, original_kind, reason, label, document_origin, completion):
            # Playwright invokes routes in independent greenlets. Capture this
            # exact page/document/case instead of borrowing the next loop values.
            def late_wire(route):
                if not require_create_destination(route):
                    return
                request = route.request
                if request.method != 'POST' or not request.headers.get('x-parent-intent'):
                    route.continue_()
                    return
                assert request.frame == original_page.main_frame
                assert original_page.evaluate('performance.timeOrigin') == document_origin
                assert completion['matched'] == 0, 'Original late retry intercepted more than once'
                body = request.post_data_json
                assert body['name'] == label and body['accountId'] == 'actor' and body['isKidsProfile'] is True
                operation = request.headers.get('x-profile-creation-operation')
                assert re.fullmatch(r'pcr_[0-9a-f]{64}', operation or '')
                assert re.fullmatch(r'pcc_[0-9a-f]{64}', request.headers['x-parent-intent'])
                assert any(entry['ref'] == operation and entry['ownerID'] == 'actor' for entry in stored())
                completion['matched'] += 1
                retained_before_retry.append(True)
                actual = route.fetch(max_redirects=0)
                assert actual.status == 200
                assert original_page.evaluate('performance.timeOrigin') == document_origin
                if reason == 'modal':
                    original_page.evaluate("hideModal();showCreateProfileModal('actor')")
                    original_page.locator('[data-profile-create] input[name=name]').fill('Replacement modal draft')
                elif reason == 'step':
                    original_page.evaluate('goToStep(3)')
                elif reason == 'refresh':
                    original_page.evaluate('loadData()' if original_kind == 'accounts' else 'loadAccountsAndProfiles()')
                elif reason == 'owner':
                    selector = 'form[data-profile-create] select[name=accountId]' if original_kind == 'accounts' else '#profileAccountId'
                    original_page.locator(selector).evaluate("""select => {
                      const next=Array.from(select.options).find(option=>option.value!==select.value);
                      if(!next) throw new Error('Synthetic second household missing');
                      select.value=next.value;select.dispatchEvent(new Event('change',{bubbles:true}));
                    }""")
                else:
                    assert reason == 'pagehide'
                    original_page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide'))")
                assert original_page.evaluate('performance.timeOrigin') == document_origin
                route.fulfill(response=actual)
                completion['fulfilled'] = True
            return late_wire

        retirements = ['refresh', 'pagehide']
        retirements.append('modal' if kind == 'accounts' else 'step')
        if args.role == 'master':
            retirements.append('owner')
        for reason in retirements:
            loaded(kind)
            label = 'Synthetic late wire ' + kind + ' ' + reason
            form = fill(kind, label)
            before = state()
            document_origin = page.evaluate('performance.timeOrigin')
            completion = {'matched': 0, 'fulfilled': False}
            late_wire = late_wire_for(page, kind, reason, label, document_origin, completion)

            page.route('**' + create_path, late_wire)
            form.locator('button[type=submit]').click()
            confirm()
            page.wait_for_function("""basePath => window.createProfileCreationClient({
              actorAccountID:'actor',basePath}).list().length === 1""", arg=prefix)
            # Retirement clears the action before an awaited refresh finishes.
            # Drain active callbacks, including their actual refresh and held
            # reply, before the next loaded() may navigate this document away.
            # wait propagates handler errors; it does not use ignoreErrors.
            page.wait_for_function('profileCreationAction === null')
            page.unroute_all(behavior='wait')
            assert completion == {'matched': 1, 'fulfilled': True}, 'Original late reply did not finish'
            assert page.evaluate('performance.timeOrigin') == document_origin
            page.route('**' + create_path, verify_retained)
            check_compound(before, state())
            assert len(stored()) == 1
            assert dialog().count() == 0
            if reason == 'modal':
                assert name_input(kind).input_value() == 'Replacement modal draft'
            if reason == 'owner':
                owner_selector = 'form[data-profile-create] select[name=accountId]' if kind == 'accounts' else '#profileAccountId'
                assert page.locator(owner_selector).input_value() != 'actor'
            checkpoints.append({'case': 'late-wire-' + kind + '-' + reason, 'created': state()['createdResults'],
                                'wireMatches': completion['matched'], 'wireFulfilled': completion['fulfilled'],
                                'callbackDrained': True})

    # Actual session revocation rejects a fresh explicit read without a prompt
    # or replay; the locally retained locator remains unresolved.
    loaded('accounts')
    form = fill('accounts', 'Synthetic auth retirement')
    assert context.request.get(args.base_url + '/fixture/mask-next').status == 204
    form.locator('button[type=submit]').click()
    confirm()
    page.wait_for_function("document.querySelector('[data-creation-read]') !== null")
    ref = stored()[0]['ref']
    page.wait_for_function("""ref => !document.querySelector('[data-creation-operation="'+ref+'"] [data-creation-read]').disabled""", arg=ref)
    before = state()
    assert context.request.get(args.base_url + '/fixture/retire-session').status == 204
    row(ref).locator('[data-creation-read]').click()
    page.wait_for_function("""ref => !document.querySelector('[data-creation-operation="'+ref+'"] [data-creation-read]').disabled""", arg=ref)
    assert any(entry['ref'] == ref for entry in stored())
    assert state()['commits'] == before['commits']
    assert dialog().count() == 0
    check_no_followups(before, state())

    assert retained_before_retry and all(retained_before_retry)
    assert all(not item['passwordPresent'] and not item['intentPresent'] and not item['operationPresent']
               for item in requests if item['path'].startswith(read_prefix))
    assert denied == [], denied
    assert errors == [], errors
    (out / 'browser-evidence.json').write_text(json.dumps({
        'role': args.role, 'cases': checkpoints, 'locatorRetainedBeforeRetries': len(retained_before_retry),
        'recordedRequests': requests, 'deniedDestinations': denied, 'pageErrors': errors,
        'boundary': 'Actual rendered templates and Cookie/PG routes; known-COMMIT HTTP masking and forced JSON completion, not DB ACK loss, full startup or device acceptance.',
    }, indent=2) + '\n')
    context.close()
    browser.close()
