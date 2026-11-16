import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright

p = argparse.ArgumentParser()
p.add_argument('--base-url', required=True)
p.add_argument('--chromium', required=True)
p.add_argument('--artifacts', required=True)
args = p.parse_args()
out = Path(args.artifacts)
out.mkdir(parents=True, exist_ok=True)

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    context.request.get(args.base_url + '/fixture/sign-in')
    page.goto(args.base_url + '/admin/tools')
    panel = page.locator('#sabFallbackPanel')
    panel.get_by_text('Saved observations. No provider check was started.', exact=True).wait_for()
    assert panel.locator('article').count() == 50
    assert panel.locator('article h3').first.inner_text() == 'Queue 53'
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 0
    assert not page.locator('#sabFallbackRows img').count()
    assert page.evaluate('window.SAB_XSS') is None
    assert 'URL-PATH-KEY-CANARY' not in panel.inner_text()
    page.screenshot(path=str(out / 'saved-1280.png'), full_page=True)
    panel.get_by_role('button', name='Next', exact=True).click()
    panel.get_by_text('Saved observations. No provider check was started.', exact=True).wait_for()
    assert panel.locator('article').count() == 3
    assert panel.locator('article h3').first.inner_text() == 'Queue 3'
    assert 'Submission acknowledgement unknown' in panel.inner_text()
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 0
    panel.get_by_role('button', name='Previous', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 53', exact=True).wait_for()
    panel.locator('article').first.get_by_role('button', name='Refresh status').click()
    panel.locator('article').first.get_by_text('Downloading', exact=True).wait_for()
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 1
    # Delayed old-page control response cannot replace the newly selected page.
    delayed = []
    page.route('**/admin/api/usenet/fallback/53/refresh', lambda route: delayed.append(route))
    panel.locator('article').first.get_by_role('button', name='Refresh status').click()
    page.wait_for_function('document.querySelector("#sabFallbackRows button").disabled')
    for _ in range(50):
        if delayed:
            break
        page.wait_for_timeout(20)
    assert delayed
    panel.get_by_role('button', name='Next', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 3', exact=True).wait_for()
    try:
        delayed.pop().fulfill(status=200, content_type='application/json', body='{"queueId":53,"state":"failed"}')
    except Exception:
        pass  # Navigation aborted the original request; no server acceptance is claimed.
    assert panel.locator('article').count() == 3
    assert panel.locator('article h3').first.inner_text() == 'Queue 3'
    page.unroute('**/admin/api/usenet/fallback/53/refresh')
    page.set_viewport_size({'width': 390, 'height': 844})
    page.wait_for_function('document.querySelector("#adminSidebar").getBoundingClientRect().right <= 0')
    panel.scroll_into_view_if_needed()
    assert panel.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
    page.screenshot(path=str(out / 'saved-390.png'), full_page=True)
    # A malformed response is visible as failure, not an empty successful page.
    page.route('**/admin/api/usenet/fallback?*', lambda route: route.fulfill(status=200, content_type='application/json', body='{"items":[{"queueId":1,"state":"<img src=x onerror=alert(1)>"}]}'))
    panel.get_by_role('button', name='Reload saved status', exact=True).click()
    panel.get_by_text('Saved observations could not be loaded. Reload saved status to try again.', exact=True).wait_for()
    assert panel.locator('article').count() == 0
    page.unroute('**/admin/api/usenet/fallback?*')
    panel.get_by_role('button', name='Reload saved status', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 53', exact=True).wait_for()
    context.request.get(args.base_url + '/fixture/demote')
    panel.get_by_role('button', name='Reload saved status', exact=True).click()
    panel.get_by_text('Access is no longer available. Reload the page after signing in.', exact=True).wait_for()
    assert panel.locator('article').count() == 0
    assert panel.get_by_role('button', name='Reload saved status', exact=True).is_disabled()
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 1
    regular = browser.new_context()
    regular.request.get(args.base_url + '/fixture/sign-in?regular=true')
    other = regular.new_page()
    other.goto(args.base_url + '/admin/tools')
    assert other.locator('#sabFallbackPanel').count() == 0
    assert not errors, errors
    (out / 'result.json').write_text(json.dumps({'fixture': 'synthetic-cookie-real-sqlite-importer-status-http', 'pageErrors': errors, 'explicitProviderCalls': 1, 'automaticProviderCalls': 0, 'widths': [1280, 390], 'lateResponseIgnored': True, 'malformedPageRejected': True, 'currentRoleDenied': True}, indent=2) + '\n')
    browser.close()
