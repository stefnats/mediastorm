"""Actual private POST joined-copy download, synthetic local containers, no device decoder claim."""
import argparse
import copy
import datetime
import ipaddress
import hashlib
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlencode

from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument('--base-url', required=True)
parser.add_argument('--chromium', required=True)
parser.add_argument('--artifacts', required=True)
args = parser.parse_args()
origin = urlsplit(args.base_url)
assert origin.scheme == 'http' and ipaddress.ip_address(origin.hostname).is_loopback and origin.username is None and origin.password is None
artifacts = Path(args.artifacts)
artifacts.mkdir(parents=True, exist_ok=True)
errors, requests, checks, denied_external = [], [], [], []
request_diagnostics = []


def wait_until(page, condition, label, seconds=15):
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, label
        page.wait_for_timeout(20)


with sync_playwright() as driver:
    browser = driver.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox'])
    context = browser.new_context(accept_downloads=True, service_workers='block')

    def local_only(route):
        url = urlsplit(route.request.url)
        if (url.scheme, url.netloc) != (origin.scheme, origin.netloc):
            denied_external.append({'scheme': url.scheme, 'host': url.hostname})
            route.abort()
        else:
            route.continue_()

    context.route('**/*', local_only)
    info = context.request.get(args.base_url + '/fixture/info').json()
    assert info['fixture'] == 'synthetic-recording-export-web'

    def stats():
        return context.request.get(args.base_url + '/fixture/derived').json()

    def fetch_local(route):
        requested = urlsplit(route.request.url)
        assert (requested.scheme, requested.netloc) == (origin.scheme, origin.netloc)
        return route.fetch(max_redirects=0)

    def watch_request(request):
        parsed = urlsplit(request.url)
        if '/derived/' not in parsed.path:
            return
        diagnostic = {'method': request.method,
                      'pathKind': 'stream' if parsed.path.endswith('/stream') else 'read' if parsed.path.endswith('/read') else 'join' if parsed.path.endswith('/join') else 'other',
                      'queryKeys': sorted(parse_qs(parsed.query, keep_blank_values=True))}
        request_diagnostics.append(diagnostic)
        (artifacts / 'derived-request-diagnostic.json').write_text(json.dumps(request_diagnostics, indent=2))
        assert request.method == 'POST' and not parsed.query, 'Unexpected joined-copy request: ' + json.dumps(diagnostic)
        fields = parse_qs(request.post_data or '', keep_blank_values=True)
        assert set(fields) <= {'profileId', 'profileUnlock'} and all(len(values) == 1 for values in fields.values())
        assert fields.get('profileId', [''])[0] in (info['locked'], info['open'])
        assert 'authorization' not in request.headers and 'x-profile-unlock' not in request.headers
        requests.append({'pathKind': 'stream' if parsed.path.endswith('/stream') else parsed.path.rsplit('/', 1)[-1],
                         'method': request.method, 'noURLCredentials': True, 'opaqueBodyUnlock': bool(fields.get('profileUnlock', [''])[0])})

    def new_page(role='account', profile=None, width=390):
        page = context.new_page()
        page.set_viewport_size({'width': width, 'height': 1000})
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.on('request', watch_request)
        page.goto(args.base_url + '/' + role + '/recordings')
        page.locator('#recordingProfile').select_option(profile or info['open'])
        row = 'job-' + (profile or info['open'])
        page.locator('[data-recording-derived="' + row + '"][data-derived-operation=read]').wait_for()
        return page, row

    def action(page, row, kind):
        return page.locator('[data-recording-derived="' + row + '"][data-derived-operation="' + kind + '"]')

    def unlock(page, wrong=False):
        dialog = page.locator('#recordingDerivedPIN')
        dialog.wait_for(state='visible')
        if wrong:
            dialog.locator('[data-derived-pin]').fill('wrong')
            dialog.get_by_role('button', name='Unlock and continue', exact=True).click()
            dialog.get_by_text('Incorrect PIN. Try again.', exact=True).wait_for()
        dialog.locator('[data-derived-pin]').fill('1234')
        dialog.get_by_role('button', name='Unlock and continue', exact=True).click()

    for role in ('admin', 'account'):
        assert context.request.get(args.base_url + '/fixture/sign-in?role=' + role).status == 204
        for width in (320, 390, 1280):
            before = stats()
            page, row = new_page(role, info['locked'], width)
            page.wait_for_timeout(100)
            assert stats() == before, 'Loading a recording automatically started derived work'
            assert not page.locator('#recordingDerivedOpen').is_visible()
            action(page, row, 'read').click()
            unlock(page, wrong=role == 'admin' and width == 320)
            wait_until(page, lambda: 'No current joined copy' in page.locator('#recordingDerivedStatus').inner_text()
                       or page.locator('#recordingDerivedOpen').is_visible(), 'explicit stored status did not finish')
            assert stats()['prepares'] == before['prepares'] and stats()['streams'] == before['streams']
            action(page, row, 'join').click()
            page.locator('#recordingDerivedOpen').wait_for(state='visible', timeout=30000)
            ready_text = page.locator('#recordingDerivedStatus').inner_text()
            assert 'Source recording is incomplete' in ready_text and 'closed gaps were omitted' in ready_text
            assert 'not a complete or seamless' in ready_text and stats()['originalBytesUnchanged']
            assert stats()['streams'] == before['streams'], 'Preparing automatically selected the derived source'
            assert page.locator('#recordingDerivedOpen').inner_text() == 'Save prepared joined copy'
            with page.expect_download(timeout=30000) as pending_download:
                page.locator('#recordingDerivedOpen').click()
            download = pending_download.value
            assert download.suggested_filename == 'recording-joined.mkv' and download.failure() is None
            downloaded = Path(download.path()).read_bytes()
            expected = context.request.get(args.base_url + '/fixture/derived-digest?' + urlencode({'recordingId': row}), max_redirects=0)
            assert expected.status == 200
            expected = expected.json()
            assert len(downloaded) == expected['bytes'] > 0 and hashlib.sha256(downloaded).hexdigest() == expected['sha256']
            download.save_as(artifacts / ('joined-copy-' + role + '-' + str(width) + '.mkv'))
            assert stats()['streams'] == before['streams'] + 1, 'Save did not use exactly one explicit private POST'
            requested = urlsplit(download.url)
            assert (requested.scheme, requested.netloc) == (origin.scheme, origin.netloc) and not requested.query
            denied_get = context.request.get(download.url, max_redirects=0)
            rejected = {'method': 'GET', 'status': denied_get.status, 'contentType': denied_get.headers.get('content-type', ''),
                        'hasRedirect': bool(denied_get.headers.get('location')), 'bodyBytes': len(denied_get.body()),
                        'authorizedStreamDelta': stats()['streams'] - before['streams']}
            (artifacts / 'derived-method-rejection.json').write_text(json.dumps(rejected, indent=2))
            assert denied_get.status == 405 and denied_get.headers.get('allow') == 'POST' and not denied_get.body()
            assert not denied_get.headers.get('location') and not denied_get.headers.get('content-disposition')
            denied_head = context.request.head(download.url, max_redirects=0)
            assert denied_head.status == 405 and denied_head.headers.get('allow') == 'POST' and not denied_head.body()
            assert not denied_head.headers.get('location') and not denied_head.headers.get('content-disposition')
            assert stats()['streams'] == before['streams'] + 1, 'GET reached authorized media service'
            assert page.locator('#recordingDerivedReceiver iframe').count() == 1
            assert not page.locator('#recordingDerivedReceiver iframe').is_visible()
            assert not page.locator('form input[name=profileUnlock]').count()
            assert page.locator('#recordingDerivedPINInput').input_value() == ''
            assert not page.evaluate('Object.keys(localStorage).some(key => /unlock|pin/i.test(key))')
            assert all('profileUnlock' not in urlsplit(frame.url).query for frame in page.frames)
            assert stats()['originalBytesUnchanged']
            if role == 'account':
                page.locator('#recordingDerivedStatus').scroll_into_view_if_needed()
                page.screenshot(path=str(artifacts / ('joined-copy-' + str(width) + '.png')))
            checks.append({'role': role, 'width': width, 'realCoreJoinAndCookiePIN': True, 'noAutomaticWorkOrSelection': True,
                           'knownGapAndIncompleteQualified': True, 'originalBytesUnchanged': True, 'privatePOSTDownload': True, 'downloadedBytes': len(downloaded), 'downloadSHA256': expected['sha256'], 'credentialFreeGETAndHEAD405WithoutMedia': True})
            page.close()

    # Cancelling the production fetch disconnects the original held join request.
    page, row = new_page()
    before = stats()
    assert context.request.get(args.base_url + '/fixture/derived-hold').status == 204
    action(page, row, 'join').click()
    wait_until(page, lambda: stats()['waiting'], 'held original join not reached')
    page.locator('[data-recording-derived-cancel]').first.click()
    wait_until(page, lambda: stats()['cancelled'] > before['cancelled'], 'Cancel did not disconnect original request')
    assert not page.locator('#recordingDerivedOpen').is_visible()
    assert context.request.get(args.base_url + '/fixture/derived-release').status == 204
    checks.append({'cancelDisconnectsOriginalJoinRequest': True, 'noProcessCancellationClaimFromHeldSeam': True})
    page.close()

    # Equal-valued replacement rows still retire their predecessor's request receipt.
    page, row = new_page()
    before = stats()
    assert context.request.get(args.base_url + '/fixture/derived-hold').status == 204
    action(page, row, 'join').click()
    wait_until(page, lambda: stats()['waiting'], 'held replacement request not reached')
    page.evaluate('''async profile => {
        const reply = await fetch('/account/api/live/recordings?profileId=' + encodeURIComponent(profile));
        const body = await reply.json();
        renderRecordings(JSON.parse(JSON.stringify(body.recordings)));
    }''', info['open'])
    wait_until(page, lambda: stats()['cancelled'] > before['cancelled'], 'equal row replacement retained old request')
    assert not page.locator('#recordingDerivedOpen').is_visible()
    assert context.request.get(args.base_url + '/fixture/derived-release').status == 204
    action(page, row, 'read').click()
    # A read is useful without a prior open-profile join and must never prepare.
    wait_until(page, lambda: 'No current joined copy' in page.locator('#recordingDerivedStatus').inner_text()
               or page.locator('#recordingDerivedOpen').is_visible(), 'fresh replacement row action did not work')
    assert stats()['prepares'] == before['prepares'] + 1
    checks.append({'equalRowReplacementRetiresOldReceipt': True, 'freshCurrentRowUsable': True})
    page.close()

    # A delayed real PIN reply cannot prepare for a replacement selected profile.
    page, row = new_page(profile=info['locked'])
    before = stats()
    action(page, row, 'join').click()
    page.locator('#recordingDerivedPIN').wait_for(state='visible')
    assert context.request.get(args.base_url + '/fixture/hold?kind=pin').status == 204
    dialog = page.locator('#recordingDerivedPIN')
    dialog.locator('[data-derived-pin]').fill('1234')
    dialog.get_by_role('button', name='Unlock and continue', exact=True).click()
    wait_until(page, lambda: context.request.get(args.base_url + '/fixture/info').json()['waitingPIN'], 'held original PIN not reached')
    page.locator('#recordingProfile').select_option(info['open'])
    assert context.request.get(args.base_url + '/fixture/release').status == 204
    action(page, 'job-' + info['open'], 'read').wait_for()
    assert not dialog.is_visible() and stats()['prepares'] == before['prepares']
    checks.append({'latePINCannotPrepareForReplacementProfile': True})
    page.close()

    # Validate the actual production JS against malformed copies of a real descriptor.
    page, row = new_page()
    action(page, row, 'join').click()
    page.locator('#recordingDerivedOpen').wait_for(state='visible', timeout=30000)
    pattern = '**/account/api/live/recordings/*/derived/read'
    mutations = {
        'opaque-id': lambda value: value.update(id='../not-an-id'),
        'wrong-recording': lambda value: value.update(recordingId='foreign-row'),
        'wrong-status': lambda value: value.update(sourceStatus='completed'),
        'incomplete-lost': lambda value: value.update(sourceIncomplete=False),
        'gaps-lost': lambda value: value.update(gapsOmitted=False),
        'zero-bytes': lambda value: value.update(bytes=0),
        'unknown-field': lambda value: value.update(privatePath='synthetic-disallowed'),
        'offset': lambda value: value['partOffsets'][1].update(startMs=1),
        'total-duration': lambda value: value.update(durationMs=value['durationMs'] + 1),
        'null-offsets': lambda value: value.update(partOffsets=None),
        'invalid-time': lambda value: value.update(observedAt='2026-02-31T00:00:00Z'),
    }
    def malformed_handler(change):
        def malformed(route):
            response = fetch_local(route)
            body = copy.deepcopy(response.json())
            change(body)
            route.fulfill(response=response, json=body)
        return malformed

    for name, mutate in mutations.items():
        before = stats()
        malformed = malformed_handler(mutate)

        page.route(pattern, malformed)
        action(page, row, 'read').click()
        wait_until(page, lambda: 'No ready result was adopted' in page.locator('#recordingDerivedStatus').inner_text(), 'malformed descriptor accepted: ' + name)
        assert not page.locator('#recordingDerivedOpen').is_visible()
        assert stats()['prepares'] == before['prepares'] and stats()['streams'] == before['streams']
        page.unroute(pattern, malformed)
        checks.append({'malformedDescriptorRejected': name, 'noMediaWorkOrAdoption': True})

    def delayed_expiry(route):
        response = fetch_local(route)
        body = response.json()
        from email.utils import parsedate_to_datetime
        server = parsedate_to_datetime(response.headers['date'])
        stamp = lambda value: value.isoformat().replace('+00:00', 'Z')
        body['observedAt'] = stamp(server - datetime.timedelta(seconds=1))
        body['expiresAt'] = stamp(server + datetime.timedelta(seconds=2))
        # Preserve the original HTTP Date while delivery exceeds its advertised lifetime.
        time.sleep(2.2)
        route.fulfill(response=response, json=body)

    page.route(pattern, delayed_expiry)
    before = stats()
    action(page, row, 'read').click()
    wait_until(page, lambda: 'expired' in page.locator('#recordingDerivedStatus').inner_text(), 'delayed expired response remained ready')
    assert not page.locator('#recordingDerivedOpen').is_visible() and stats()['streams'] == before['streams']
    page.unroute(pattern, delayed_expiry)
    checks.append({'originalHTTPDatePlusRTTRejectsDelayedExpiry': True})
    assert stats()['originalBytesUnchanged']
    page.close()
    assert not errors, errors
    assert not denied_external, denied_external
    context.close()
    browser.close()

(artifacts / 'results.json').write_text(json.dumps({'checks': checks, 'requests': requests, 'pageErrors': errors,
    'scope': 'Actual production template/JS, real cookie/PIN route registration, attachment-only web adapter, derived handlers/core joiner, downloaded bytes/hash and original local files. Tiny synthetic containers only. Held seam tests prove HTTP cancellation, not subprocess cancellation. Mutated descriptor cases prove browser rejection only. No physical decoder, seamless timeline or restored missing-content claim.'}, indent=2) + '\n')
