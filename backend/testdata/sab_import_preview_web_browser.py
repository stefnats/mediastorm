import argparse
import copy
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
    denied, errors = [], []

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
    first = panel.locator('article').first
    info = lambda: context.request.get(args.base_url + '/fixture/preview-info').json()
    assert info() == {'previews': 0, 'provider': 0, 'copies': 0, 'reads': 0, 'denied': 0}
    assert first.get_by_role('button', name='Copy files', exact=True).is_hidden()
    first.get_by_role('button', name='View saved copy', exact=True).click()
    first.get_by_text('Copy files is not set up.', exact=True).wait_for()
    assert info()['provider'] == info()['previews'] == 0
    first.get_by_role('button', name='Inspect local files', exact=True).click()
    first.get_by_text('1 regular file(s) observed; 28 bytes.', exact=True).wait_for()
    assert 'Possible media (MKV), 28 bytes.' in first.inner_text()
    assert info() == {'previews': 1, 'provider': 2, 'copies': 0, 'reads': 1, 'denied': 0}
    assert 'PRIVATE' not in panel.inner_text()
    assert not panel.locator('img').count()
    assert page.evaluate('window.COPY_XSS') is None
    assert first.get_by_role('button', name='Copy files', exact=True).is_hidden()
    first.screenshot(path=str(out / 'inspect-1280.png'))
    page.set_viewport_size({'width': 390, 'height': 844})
    page.wait_for_function('document.querySelector("#adminSidebar").getBoundingClientRect().right <= 0')
    first.scroll_into_view_if_needed()
    assert first.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
    first.screenshot(path=str(out / 'inspect-390.png'))

    valid = {'queueId': 53, 'state': 'local_inventory', 'observedAt': '2026-11-17T04:00:00Z', 'entryCount': 1, 'observedBytes': 28, 'files': [{'ordinal': 1, 'kind': 'media_candidate', 'extension': 'mkv', 'observedBytes': 28}]}
    malformed = []
    for mutation in ('sum', 'ordinal', 'kind', 'count', 'fraction', 'date', 'private-field', 'empty'):
        value = copy.deepcopy(valid)
        if mutation == 'sum':
            value['observedBytes'] = 29
        elif mutation == 'ordinal':
            value['files'][0]['ordinal'] = 2
        elif mutation == 'kind':
            value['files'][0]['kind'] = 'other_regular'
        elif mutation == 'count':
            value['entryCount'] = 2
        elif mutation == 'fraction':
            value['files'][0]['observedBytes'] = 27.5
        elif mutation == 'date':
            value['observedAt'] = '2026-02-30T04:00:00Z'
        elif mutation == 'private-field':
            value['files'][0]['path'] = '<img src=x onerror=window.PREVIEW_XSS=true>'
        elif mutation == 'empty':
            value['entryCount'], value['observedBytes'], value['files'] = 0, 0, []
        malformed.append(value)
    pattern = '**/admin/api/usenet/fallback/53/import-preview'
    def malformed_reply(value):
        body = json.dumps(value)
        return lambda route: route.fulfill(status=200, content_type='application/json', body=body)
    for value in malformed:
        page.route(pattern, malformed_reply(value))
        first.get_by_role('button', name='Inspect local files', exact=True).click()
        first.get_by_text('File details could not be inspected. No copy was requested.', exact=True).wait_for()
        assert not first.locator('li').count()
        assert not first.locator('img').count()
        assert first.get_by_role('button', name='Copy files', exact=True).is_hidden()
        page.unroute(pattern)
    assert page.evaluate('window.PREVIEW_XSS') is None
    assert info()['previews'] == 1 and info()['copies'] == 0

    delayed = []

    def hold(route):
        delayed.append((route, route.fetch()))

    page.route(pattern, hold)
    first.get_by_role('button', name='Inspect local files', exact=True).click()
    for _ in range(100):
        if delayed:
            break
        page.wait_for_timeout(20)
    assert delayed
    assert first.get_by_role('button', name='Inspect local files', exact=True).is_disabled()
    assert first.get_by_role('button', name='View saved copy', exact=True).is_disabled()
    assert first.get_by_role('button', name='Refresh status', exact=True).is_disabled()
    panel.get_by_role('button', name='Next', exact=True).click()
    panel.locator('article h3').first.get_by_text('Queue 3', exact=True).wait_for()
    route, response = delayed.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass  # Page replacement canceled the original browser fetch.
    page.unroute(pattern)
    assert 'regular file(s) observed' not in panel.inner_text()
    assert panel.locator('article').count() == 3
    assert info()['previews'] == 2 and info()['provider'] == 4 and info()['copies'] == 0
    context.request.get(args.base_url + '/fixture/demote')
    panel.locator('article').first.get_by_role('button', name='Inspect local files', exact=True).click()
    panel.get_by_text('Access is no longer available. Reload the page after signing in.', exact=True).wait_for()
    assert panel.locator('article').count() == 0
    final = info()
    assert final == {'previews': 3, 'provider': 4, 'copies': 0, 'reads': 1, 'denied': 0}
    assert not errors and not denied
    (out / 'result.json').write_text(json.dumps({'fixture': 'actual-cookie-sqlite-fd-metadata-preview', 'widths': [1280, 390], 'pageErrors': errors, 'blockedPageRequests': denied, 'automaticPreviewRequests': 0, 'actualPreviewAttempts': final['previews'], 'providerHTTP': final['provider'], 'copyRequests': 0, 'copyConfigurationDisabled': True, 'malformedResponsesRejected': len(malformed), 'oldPageResponseIgnored': True, 'currentRoleDenied': True}, indent=2) + '\n')
    browser.close()
