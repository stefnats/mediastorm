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
args = parser.parse_args()
base = urlparse(args.base_url)
assert base.scheme == 'http' and ipaddress.ip_address(base.hostname).is_loopback
out = Path(args.artifacts)
out.mkdir(parents=True, exist_ok=True)
password = 'synthetic-parent-password'

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        executable_path=args.chromium, headless=True,
        args=['--no-sandbox', '--disable-background-networking',
              '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1100, 'height': 900})
    page_errors, blocked_requests = [], []

    def same_fixture_only(route):
        target = urlparse(route.request.url)
        if target.scheme == 'http' and target.netloc == base.netloc:
            route.continue_()
        else:
            blocked_requests.append('non-fixture destination')
            route.abort()

    context.route('**/*', same_fixture_only)
    page = context.new_page()
    page.on('pageerror', lambda error: page_errors.append(str(error)))

    def state():
        response = context.request.get(args.base_url + '/fixture/state')
        assert response.status == 200
        return response.json()

    initial = state()
    profile, guide = initial['profile'], initial['guide']
    endpoint = '/api/users/' + profile + '/kids/live-policy'
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    enabled = page.locator('#kids-live-enabled')
    age = page.locator('#kids-live-age')
    guide_select = page.locator('#kids-live-guide')
    save = page.locator('#kids-live-save')
    def wait_status(text):
        page.wait_for_function('(expected) => document.getElementById("kids-live-status").textContent === expected', arg=text)

    def confirm():
        dialog().wait_for()
        assert dialog().count() == 1
        dialog().locator('input[type=password]').fill(password)
        dialog().get_by_role('button', name='Confirm', exact=True).click()
        dialog().wait_for(state='detached')

    page.goto(args.base_url + '/component?profileId=' + profile)
    page.wait_for_function('!document.querySelector("#kids-live-fields").disabled')
    assert page.locator('script[src="/api/static/account-parent-auth-v1.js"]').count() == 1
    assert page.locator('script[src="/api/static/kids-live-policy-v1.js"]').count() == 1
    assert page.evaluate('typeof window.parentModeFetch') == 'function'
    assert enabled.is_checked() is False and age.input_value() == '12'
    assert guide_select.input_value() == ''
    assert guide_select.locator('option').count() == 2
    assert state()['attempts'] == 0 and state()['acceptedWrites'] == 0
    assert state()['guideRequests'] == 1
    page.screenshot(path=str(out / 'saved-disabled-rule.png'))

    # Enabling requires the parent's conscious selection of the imported guide.
    enabled.check()
    age.select_option('6')
    save.click()
    wait_status('Choose a loaded guide before enabling Live TV.')
    assert dialog().count() == 0 and state()['attempts'] == 0
    guide_select.select_option(guide)
    before_enable = state()
    save.click()
    dialog().wait_for()
    assert state()['acceptedWrites'] == before_enable['acceptedWrites']
    assert save.is_disabled()
    dialog().screenshot(path=str(out / 'actual-parent-password-dialog.png'))
    confirm()
    wait_status('Live TV rule saved. Unknown and higher-rated programmes remain blocked.')
    after_enable = state()
    assert after_enable['policy'] == {'enabled': True, 'country': 'DE', 'maximumAge': 6, 'guideSourceId': guide}
    assert after_enable['attempts'] == before_enable['attempts'] + 2
    assert after_enable['confirmed'] == before_enable['confirmed'] + 1
    assert after_enable['acceptedWrites'] == before_enable['acceptedWrites'] + 1

    # Explicit off uses the same real password helper and actual handler/storage.
    enabled.uncheck()
    before_disable = state()
    save.click()
    confirm()
    wait_status('Live TV is now off for this profile.')
    after_disable = state()
    assert after_disable['policy']['enabled'] is False
    assert after_disable['policy']['guideSourceId'] == guide
    assert after_disable['acceptedWrites'] == before_disable['acceptedWrites'] + 1
    assert after_disable['confirmed'] == before_disable['confirmed'] + 1

    # Drop only the HTTP reply AFTER the actual local handler saved successfully.
    # This models a lost browser response, not an uncertain database COMMIT.
    page.set_viewport_size({'width': 390, 'height': 844})
    discarded = []

    def drop_success_reply(route):
        response = route.fetch()
        if route.request.method == 'PUT' and route.request.headers.get('x-account-password') and response.status == 200:
            discarded.append(True)
            route.abort('connectionclosed')
        else:
            route.fulfill(response=response)

    pattern = '**' + endpoint
    page.route(pattern, drop_success_reply)
    enabled.check()
    before_loss = state()
    save.click()
    dialog().wait_for()
    assert dialog().evaluate('(element) => element.getBoundingClientRect().left >= 0 && element.getBoundingClientRect().right <= innerWidth')
    confirm()
    wait_status('The save result is unknown. Reload the saved rule; no save will be repeated automatically.')
    after_loss = state()
    assert discarded == [True]
    assert after_loss['policy']['enabled'] is True
    assert after_loss['attempts'] == before_loss['attempts'] + 2
    assert after_loss['acceptedWrites'] == before_loss['acceptedWrites'] + 1
    assert after_loss['confirmed'] == before_loss['confirmed'] + 1
    assert save.is_disabled() and page.locator('#kids-live-reload').is_enabled()
    page.screenshot(path=str(out / 'lost-reply-save-disabled.png'))

    # Even a repeated submit event must not dispatch another mutation now.
    page.locator('#kids-live-policy-form').dispatch_event('submit')
    assert save.is_disabled()
    assert state()['attempts'] == after_loss['attempts']
    assert state()['acceptedWrites'] == after_loss['acceptedWrites']
    page.unroute(pattern, drop_success_reply)
    page.locator('#kids-live-reload').click()
    wait_status('Saved rule loaded. Playback uses only the selected guide.')
    page.wait_for_function('!document.querySelector("#kids-live-fields").disabled')
    reloaded = state()
    assert enabled.is_checked() and age.input_value() == '6' and guide_select.input_value() == guide
    assert not save.is_disabled()
    assert reloaded['policyReads'] == after_loss['policyReads'] + 1
    assert reloaded['attempts'] == after_loss['attempts']
    assert reloaded['acceptedWrites'] == after_loss['acceptedWrites']
    assert reloaded['guideRequests'] == 1
    assert not page_errors and not blocked_requests
    (out / 'result.json').write_text(json.dumps({
        'fixture': 'actual-live-rule-section-js-handler-file-storage-and-cached-guide',
        'savedDisabledRuleLoaded': True,
        'explicitGuideAndRealPasswordEnable': True,
        'explicitDisableSaved': True,
        'lostReplyAfterActualSaveBlocksFurtherSubmit': True,
        'reloadReadsSavedStateWithoutSecondWrite': True,
        'noAdditionalGuideFetch': True,
        'pageErrors': page_errors,
        'blockedRequests': blocked_requests,
        'qualification': 'Synthetic authenticated-account context and local file stores. The real parent password helper/verifier is used; this does not test login, PG admission or actual broadcast playback.'
    }, indent=2) + '\n')
    context.close()
    browser.close()
