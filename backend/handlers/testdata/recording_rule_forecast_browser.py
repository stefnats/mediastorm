"""Real recording template, cookie routes and ephemeral synthetic rule matches."""
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
        page.goto(base + '/admin/recordings')
        assert page.url == base + '/admin/recordings'
        page.locator('#recordingChannel option').first.wait_for(state='attached')
        page.locator('#recordingProfile').select_option(fixture['viewer'])
        page.locator('#recordingRuleForecastItems').get_by_text('Synthetic upcoming programme', exact=True).wait_for()
        page.evaluate('''fixture => {
            document.getElementById('recordingStart').value = formatDateForInput(new Date(fixture.start));
            document.getElementById('recordingEnd').value = formatDateForInput(new Date(fixture.stop));
        }''', fixture)
    for width in [320,375,390,414,768,1280]:
        page = context.new_page()
        page.set_viewport_size({'width': width, 'height': 1000})
        page.on('pageerror', lambda error: errors.append(str(error)))
        prepare(page)
        before = info()['writes']
        items = page.locator('#recordingRuleForecastItems')
        assert 'Forecast only' in items.inner_text()
        assert items.get_by_role('button').count() == 0
        assert items.evaluate('(e) => e.getBoundingClientRect().left >= 0 && e.getBoundingClientRect().right <= innerWidth')
        assert '1 projected rule matches' in page.locator('#recordingRuleForecastStatus').inner_text()
        assert 'synthetic-private-foreign-rule' not in page.content()
        page.get_by_role('button', name='Schedule Manual Block', exact=True).click()
        dialog = page.get_by_role('dialog', name='Recording capacity forecast')
        dialog.wait_for()
        assert '1 projected rule matches' in dialog.inner_text()
        assert 'No connection is reserved' in dialog.inner_text()
        assert info()['writes'] == before
        if width in [390,1280]:
            page.screenshot(path=str(artifacts / ('rule-forecast-warning-' + str(width) + '.png')))
        page.keyboard.press('Escape')
        dialog.wait_for(state='detached')
        items.scroll_into_view_if_needed()
        if width in [390,1280]:
            page.screenshot(path=str(artifacts / ('rule-forecast-list-' + str(width) + '.png')))
        assert info()['writes'] == before
        page.locator('#recordingProfile').select_option(fixture['other'])
        page.wait_for_function("document.getElementById('recordingRuleForecastStatus').textContent.startsWith('0 projected rule matches')")
        assert items.inner_text() == ''
        assert info()['writes'] == before
        checks.append({'width': width, 'real_template': True, 'real_cookie_guard': True, 'own_projection_shown': True, 'no_job_actions_for_projection': True, 'other_profile_projection_cleared': True, 'preview_and_cancel_do_not_write': True, 'fits': True})
        page.close()
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 1000})
    page.on('pageerror', lambda error: errors.append(str(error)))
    prepare(page)
    before = info()['writes']
    def partial(route):
        response = route.fetch()
        data = response.json()
        data['complete'] = False
        data['ruleForecast']['status'] = 'partial'
        data['projectedRecordings'][0]['title'] = '<img src=x onerror="window.ruleForecastInjected=true">'
        route.fulfill(response=response, json=data)
    page.route('**/recordings/plan?*', partial)
    page.get_by_role('button', name='Refresh', exact=True).last.click()
    page.locator('#recordingRuleForecastStatus').get_by_text('Missing guide coverage remains uncertain.', exact=False).wait_for()
    assert page.locator('#recordingRuleForecastItems img').count() == 0
    assert page.evaluate('window.ruleForecastInjected !== true')
    assert info()['writes'] == before
    page.unroute('**/recordings/plan?*')
    def legacy(route):
        response = route.fetch()
        data = response.json()
        data.pop('ruleForecast', None)
        data.pop('projectedRecordings', None)
        route.fulfill(response=response, json=data)
    page.route('**/recordings/plan?*', legacy)
    page.get_by_role('button', name='Refresh', exact=True).last.click()
    page.get_by_text('Future rule matches are not included by this server.', exact=True).wait_for()
    assert page.locator('#recordingRuleForecastItems').inner_text() == ''
    page.unroute('**/recordings/plan?*')
    page.route('**/recordings/plan?*', lambda route: route.fulfill(status=503, body='unavailable'))
    page.get_by_role('button', name='Refresh', exact=True).last.click()
    page.get_by_text('Rule forecast unavailable. No recording has been changed.', exact=True).wait_for()
    assert info()['writes'] == before
    page.close()
    browser.close()
assert not errors, errors
(artifacts / 'rule-forecast-browser-results.json').write_text(json.dumps({
    'passed': True, 'width_checks': checks, 'partial_coverage_explained': True,
    'projection_title_escaped': True, 'legacy_response_supported': True,
    'unavailable_plan_preserves_schedule': True, 'synthetic_only': True,
    'full_application_login_tested': False, 'native_ui_rendered': False,
    'intercepted_ui_edge_case_responses': ['partial', 'legacy', 'unavailable'],
    'page_errors': errors
}, indent=2) + '\n')
