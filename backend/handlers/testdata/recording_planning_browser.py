"""Exercise the real recording page and authenticated handlers using synthetic jobs."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument('--base-url', required=True)
parser.add_argument('--chromium', required=True)
parser.add_argument('--artifacts', required=True)
options = parser.parse_args()
assert urlsplit(options.base_url).hostname in ['127.0.0.1', 'localhost']
artifacts = Path(options.artifacts)
artifacts.mkdir(parents=True, exist_ok=True)
errors = []
checks = []
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=options.chromium, headless=True, args=['--no-sandbox'])
    context = browser.new_context()
    base = options.base_url
    def info(): return context.request.get(base + '/fixture/info').json()
    fixture = info()
    assert fixture['fixture'] == 'synthetic-recording-plan'
    assert context.request.get(base + '/admin/api/live/recordings/plan?profileId=' + fixture['viewer']).status != 200
    assert context.request.get(base + '/fixture/sign-in').status == 204
    def prepare(page):
        page.add_init_script("window.recordingFixtureErrors = []; addEventListener('error', e => recordingFixtureErrors.push(e.message));")
        page.goto(base + '/admin/recordings')
        assert page.url == base + '/admin/recordings', 'Fixture did not reach the authenticated recording page'
        page.wait_for_function("document.querySelector('#recordingChannel option') || document.getElementById('scheduleMessage')?.textContent || window.recordingFixtureErrors?.length")
        if not page.locator('#recordingChannel option').count():
            raise AssertionError({'page_errors': errors, 'script_errors': page.evaluate('recordingFixtureErrors'), 'message': page.locator('#scheduleMessage').inner_text()})
        page.locator('#recordingChannel option').first.wait_for(state='attached')
        page.locator('#recordingProfile').select_option(fixture['viewer'])
        page.wait_for_function('channels.length === 2')
        page.locator('#recordingsList').get_by_text('Recording capacity conflict', exact=False).first.wait_for()
        page.evaluate('''(fixture) => {
            document.getElementById('recordingStart').value = formatDateForInput(new Date(fixture.start));
            document.getElementById('recordingEnd').value = formatDateForInput(new Date(fixture.stop));
            document.getElementById('recordingTitle').value = 'Synthetic new recording';
        }''', fixture)
    def open_forecast(page):
        page.get_by_role('button', name='Schedule Manual Block', exact=True).click()
        dialog = page.get_by_role('dialog', name='Recording capacity forecast')
        dialog.wait_for()
        return dialog
    for width in [320,375,390,414,768,1280]:
        page = context.new_page()
        page.set_viewport_size({'width': width, 'height': 1000})
        page.on('pageerror', lambda error: errors.append(str(error)))
        prepare(page)
        before = info()['writes']
        dialog = open_forecast(page)
        assert 'capacity conflict' in dialog.inner_text()
        assert 'No connection is reserved' in dialog.inner_text()
        assert dialog.evaluate('(e) => e.getBoundingClientRect().left >= 0 && e.getBoundingClientRect().right <= innerWidth')
        assert page.evaluate('document.activeElement.textContent') == 'Cancel'
        assert info()['writes'] == before
        if width in [390,1280]:
            page.screenshot(path=str(artifacts / ('recording-plan-' + str(width) + '.png')), full_page=False)
        page.keyboard.press('Escape')
        dialog.wait_for(state='detached')
        assert info()['writes'] == before
        dialog = open_forecast(page)
        dialog.get_by_role('button', name='Cancel', exact=True).click()
        dialog.wait_for(state='detached')
        assert info()['writes'] == before
        checks.append({'width': width, 'real_template': True, 'authenticated_handlers': True, 'conflict_visible': True, 'preview_has_no_writes': True, 'cancel_and_escape_preserve_schedule': True, 'fits': True})
        page.close()
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 1000})
    page.on('pageerror', lambda error: errors.append(str(error)))
    prepare(page)
    before = info()['writes']
    dialog = open_forecast(page)
    dialog.get_by_role('button', name='Check again', exact=True).click()
    dialog = page.get_by_role('dialog', name='Recording capacity forecast')
    dialog.wait_for()
    assert info()['writes'] == before
    dialog.get_by_role('button', name='Schedule anyway', exact=True).click()
    page.get_by_text('Manual recording scheduled.', exact=True).wait_for()
    assert info()['writes'] == before + 1
    prepare(page)
    before = info()['writes']
    dialog = open_forecast(page)
    page.evaluate("document.getElementById('recordingTitle').value = 'Changed'; document.getElementById('recordingTitle').dispatchEvent(new Event('input', {bubbles:true}))")
    dialog.wait_for(state='detached')
    assert info()['writes'] == before
    prepare(page)
    dialog = open_forecast(page)
    page.evaluate("other => { document.getElementById('recordingProfile').value = other; document.getElementById('recordingProfile').dispatchEvent(new Event('change', {bubbles:true})); }", fixture['other'])
    dialog.wait_for(state='detached')
    assert info()['writes'] == before
    prepare(page)
    preview_requests = []
    page.on('request', lambda request: preview_requests.append(request.url) if request.url.endswith('/recordings/preview') else None)
    page.evaluate('scheduleManualRecording(); scheduleManualRecording();')
    dialog = page.get_by_role('dialog', name='Recording capacity forecast')
    dialog.wait_for()
    assert len(preview_requests) == 1
    dialog.get_by_role('button', name='Cancel', exact=True).click()
    assert info()['writes'] == before
    prepare(page)
    delayed = []
    page.route('**/recordings/preview', lambda route: delayed.append(route))
    page.get_by_role('button', name='Schedule Manual Block', exact=True).click()
    for _ in range(500):
        if delayed:
            break
        page.wait_for_timeout(10)
    assert len(delayed) == 1
    page.evaluate("other => { document.getElementById('recordingProfile').value = other; document.getElementById('recordingProfile').dispatchEvent(new Event('change', {bubbles:true})); }", fixture['other'])
    delayed[0].fulfill(status=200, content_type='application/json', body=json.dumps({'complete': True, 'candidate': {'state': 'clear'}}))
    page.unroute('**/recordings/preview')
    assert page.get_by_role('dialog').count() == 0
    assert info()['writes'] == before
    prepare(page)
    page.route('**/recordings/preview', lambda route: route.fulfill(status=503, body='unavailable'))
    dialog = open_forecast(page)
    assert 'unavailable' in dialog.inner_text()
    dialog.get_by_role('button', name='Cancel', exact=True).click()
    assert info()['writes'] == before
    page.unroute('**/recordings/preview')
    # A stale warning must be checked again. Advance only the browser's wall
    # clock; the synthetic server schedule and real handler remain unchanged.
    prepare(page)
    dialog = open_forecast(page)
    page.evaluate('window.originalPlanNow = Date.now; Date.now = () => originalPlanNow() + 16000')
    page.wait_for_function("""document.querySelector('dialog[aria-label="Recording capacity forecast"] button:last-child').disabled""")
    assert 'expired' in dialog.inner_text()
    assert info()['writes'] == before
    page.keyboard.press('Escape')
    page.evaluate('Date.now = originalPlanNow')
    # A rejected preview cannot be overridden by the UI.
    page.route('**/recordings/preview', lambda route: route.fulfill(status=403, body='forbidden'))
    page.get_by_role('button', name='Schedule Manual Block', exact=True).click()
    page.get_by_text('Recording preview was rejected.', exact=False).wait_for()
    assert page.get_by_role('dialog').count() == 0
    assert info()['writes'] == before
    page.unroute('**/recordings/preview')
    # An EPG schedule goes through the same forecast before any POST to /epg.
    prepare(page)
    page.evaluate('(fixture) => { currentEPGProgram = {title:"Synthetic EPG",start:fixture.start,stop:fixture.stop}; }', fixture)
    page.get_by_role('button', name='Schedule From EPG', exact=True).click()
    dialog = page.get_by_role('dialog', name='Recording capacity forecast')
    dialog.wait_for()
    dialog.get_by_role('button', name='Cancel', exact=True).click()
    assert info()['writes'] == before
    page.close()
    browser.close()
assert not errors, errors
(artifacts / 'recording-plan-browser-results.json').write_text(json.dumps({
    'passed': True, 'width_checks': checks, 'explicit_confirmation_schedules_once': True,
    'retry_does_not_schedule': True, 'double_click_checks_once': True, 'delayed_response_after_profile_change_cannot_schedule': True, 'changed_form_and_profile_invalidate': True,
    'unavailable_preview_requires_choice': True, 'expired_preview_requires_recheck': True,
    'rejected_preview_cannot_be_overridden': True, 'epg_uses_same_preview': True,
    'synthetic_only': True, 'full_application_login_tested': False,
    'native_ui_rendered': False, 'page_errors': errors
}, indent=2) + '\n')
