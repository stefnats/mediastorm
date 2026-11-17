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

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox', '--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
    denied = []
    errors = []
    def guard(route):
        target = urlparse(route.request.url)
        if target.scheme == 'http' and target.netloc == base.netloc:
            route.continue_()
        else:
            denied.append(target.scheme + ':blocked')
            route.abort()
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    context.route('**/*', guard)
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    page.goto(args.base_url + '/admin/tools')
    panel = page.locator('#sabFallbackPanel')
    panel.get_by_text('Saved observations. No provider check was started.', exact=True).wait_for()
    assert panel.locator('article').count() == 50
    first = panel.locator('article').first
    assert first.locator('h3').inner_text() == 'Queue 53'
    counts = context.request.get(args.base_url + '/fixture/info').json()
    assert counts['provider'] == counts['copies'] == counts['reads'] == 0
    first.get_by_role('button', name='View saved copy', exact=True).click()
    first.get_by_text('Saved copy status is unknown.', exact=True).wait_for()
    assert 'Counted toward' not in first.inner_text()
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 0
    page.once('dialog', lambda dialog: dialog.accept())
    first.get_by_role('button', name='Copy files', exact=True).click()
    first.get_by_text('A saved copy was recorded. This does not mean ready to play.', exact=False).wait_for()
    counts = context.request.get(args.base_url + '/fixture/info').json()
    assert counts['provider'] == 2 and counts['copies'] == 1 and counts['reads'] == 1
    assert first.get_by_role('button', name='Copy files', exact=True).is_hidden()
    first.get_by_role('button', name='View saved copy', exact=True).click()
    first.get_by_text('A saved copy was recorded. This does not mean ready to play.', exact=False).wait_for()
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 2
    duplicate = context.request.post(args.base_url + '/admin/api/usenet/fallback/53/stage', headers={'Origin': args.base_url})
    assert duplicate.status == 409
    assert context.request.get(args.base_url + '/fixture/info').json()['provider'] == 2
    assert not page.locator('#sabFallbackRows img').count()
    assert page.evaluate('window.COPY_XSS') is None
    assert 'PRIVATE' not in panel.inner_text()
    page.screenshot(path=str(out / 'copy-1280.png'), full_page=True)
    panel.get_by_role('button', name='Next', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 3', exact=True).wait_for()
    assert panel.locator('article').count() == 3
    delayed = []
    def hold(route):
        response = route.fetch()
        delayed.append((route, response))
    page.route('**/admin/api/usenet/fallback/3/stage', hold)
    panel.locator('article').first.get_by_role('button', name='View saved copy', exact=True).click()
    for _ in range(100):
        if delayed:
            break
        page.wait_for_timeout(20)
    assert delayed
    panel.get_by_role('button', name='Previous', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 53', exact=True).wait_for()
    route, response = delayed.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass  # The original fetch was canceled on page replacement.
    page.unroute('**/admin/api/usenet/fallback/3/stage', hold)
    assert panel.locator('article').count() == 50
    assert panel.locator('article h3').first.inner_text() == 'Queue 53'
    assert 'No saved copy is recorded for this location.' not in panel.inner_text()
    first = panel.locator('article').first
    first.get_by_role('button', name='View saved copy', exact=True).click()
    first.get_by_text('A saved copy was recorded. This does not mean ready to play.', exact=False).wait_for()
    page.set_viewport_size({'width': 390, 'height': 844})
    page.wait_for_function('document.querySelector("#adminSidebar").getBoundingClientRect().right <= 0')
    panel.scroll_into_view_if_needed()
    assert panel.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
    page.screenshot(path=str(out / 'copy-390.png'), full_page=True)
    malformed = {'queueId': 53, 'state': 'copy_recorded', 'configured': True, 'chargedBytes': -1, 'fileCount': 1, 'observedAt': '2026-02-30T10:00:00Z', 'path': '<img src=x onerror=window.COPY_XSS=true>'}
    page.route('**/admin/api/usenet/fallback/53/stage', lambda route: route.fulfill(status=200, content_type='application/json', body=json.dumps(malformed)))
    first.get_by_role('button', name='View saved copy', exact=True).click()
    first.get_by_text('Saved copy status could not be loaded.', exact=True).wait_for()
    assert first.get_by_role('button', name='Copy files', exact=True).is_hidden()
    assert not first.locator('img').count()
    page.unroute('**/admin/api/usenet/fallback/53/stage')
    context.request.get(args.base_url + '/fixture/demote')
    first.get_by_role('button', name='View saved copy', exact=True).click()
    panel.get_by_text('Access is no longer available. Reload the page after signing in.', exact=True).wait_for()
    assert panel.locator('article').count() == 0
    assert panel.get_by_role('button', name='Reload saved status', exact=True).is_disabled()
    final = context.request.get(args.base_url + '/fixture/info').json()
    assert final['provider'] == 2 and final['copies'] == 2 and final['denied'] == 0
    regular = browser.new_context()
    regular.route('**/*', guard)
    regular.request.get(args.base_url + '/fixture/sign-in?regular=true')
    other = regular.new_page()
    other.goto(args.base_url + '/admin/tools')
    assert other.locator('#sabFallbackPanel').count() == 0
    assert not errors, errors
    assert not denied, denied
    (out / 'result.json').write_text(json.dumps({'fixture': 'actual-cookie-sqlite-fd-loopback-copy', 'pageErrors': errors, 'blockedPageRequests': denied, 'providerHTTP': final['provider'], 'copyRequests': final['copies'], 'automaticCopyRequests': 0, 'widths': [1280, 390], 'passiveUnknownOmittedCounts': True, 'explicitConfirmation': True, 'lateOldPageIgnored': True, 'malformedRejected': True, 'currentRoleDenied': True}, indent=2) + '\n')
    browser.close()
