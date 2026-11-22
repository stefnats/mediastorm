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
parser.add_argument('--profile', required=True)
args = parser.parse_args()
base = urlparse(args.base_url)
assert base.scheme == 'http' and ipaddress.ip_address(base.hostname).is_loopback
out = Path(args.artifacts)
out.mkdir(parents=True, exist_ok=True)
password = 'synthetic-parent-password'

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True,
                                args=['--no-sandbox', '--disable-background-networking',
                                      '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    errors, denied, requests = [], [], []

    def guard(route):
        target = urlparse(route.request.url)
        if target.scheme == 'http' and target.netloc == base.netloc:
            route.continue_()
        else:
            denied.append(target.scheme + ':blocked')
            route.abort()

    context.route('**/*', guard)
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))

    def record(request):
        if '/api/users/' in urlparse(request.url).path and request.method != 'GET':
            requests.append({'path': urlparse(request.url).path, 'body': request.post_data,
                             'intent': request.headers.get('x-parent-intent')})

    page.on('request', record)
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    mode_button = lambda: page.locator('#save-mode')
    pair_button = lambda: page.locator('#save-rating-pair')
    movie = lambda: page.locator('#movieRating')
    series = lambda: page.locator('#tvRating')

    def settled():
        page.wait_for_function('!document.querySelector("#save-mode").disabled && !document.querySelector("#save-rating-pair").disabled')

    def load(prefix):
        page.goto(args.base_url + prefix + '/kids-settings?profileId=' + args.profile)
        settled()
        page.get_by_text('Live TV is off for this profile.', exact=True).wait_for()
        assert page.evaluate('typeof window.parentModeFetch') == 'function'
        assert page.evaluate('typeof window.parentRatingFetch') == 'function'

    def select_mode(value):
        page.locator('input[name="kidsMode"][value="' + value + '"]').check()

    def unchanged(before):
        after = state()
        assert after['rowDigest'] == before['rowDigest'], (before, after)
        assert after['modeWrites'] == before['modeWrites']
        assert after['pairWrites'] == before['pairWrites']
        assert after['unexpected'] == 0
        return after

    def start(button, before, kind):
        count = len(requests)
        button.click()
        dialog().wait_for()
        assert dialog().count() == 1
        unchanged(before)
        assert len(requests) == count + 1
        assert requests[-1]['intent'] is None
        assert requests[-1]['path'].endswith('/kids/' + kind)
        return count

    def confirm(before, kind, count):
        dialog().locator('input[type="password"]').fill(password)
        dialog().get_by_role('button', name='Confirm', exact=True).click()
        dialog().wait_for(state='detached')
        page.get_by_text('Mode saved' if kind == 'mode' else 'Ratings saved', exact=True).wait_for()
        settled()
        after = state()
        assert after[kind + 'Writes'] == before[kind + 'Writes'] + 1
        assert after[kind + 'Retries'] == before[kind + 'Retries'] + 1
        assert len(requests) == count + 2
        assert requests[-1]['body'] == requests[-2]['body']
        token_prefix = 'pmi_' if kind == 'mode' else 'prp_'
        assert requests[-1]['intent'].startswith(token_prefix)
        assert password not in page.content()
        return after

    for prefix in ['/admin', '/account']:
        for width in [1280, 390]:
            page.set_viewport_size({'width': width, 'height': 900 if width == 1280 else 844})
            assert context.request.get(args.base_url + '/fixture/seed').status == 204
            initial = state()
            load(prefix)
            unchanged(initial)
            assert initial['mode'] == 'rating' and initial['movie'] == 'G'
            assert initial['series'] == 'TV-Y' and initial['generic'] == 'PG'
            select_mode('rating_de')
            assert page.locator('#german-rating-note').is_visible()
            assert movie().input_value() == 'G' and series().input_value() == 'TV-Y'
            assert movie().locator('option:checked').text_content() == 'Saved value: G'
            assert series().locator('option:checked').text_content() == 'Saved value: TV-Y'
            unchanged(initial)

            # A newly selected mode cannot be silently saved by the pair button.
            movie().select_option('6')
            series().select_option('12')
            pair_attempts = state()['pairAttempts']
            pair_button().click()
            page.get_by_text('Confirm and save the selected mode first, then save its separate age limits.', exact=True).wait_for()
            assert state()['pairAttempts'] == pair_attempts
            unchanged(initial)

            # Both Cancel and Escape end an actual first challenge without a write.
            before = state()
            start(mode_button(), before, 'mode')
            if width == 1280:
                dialog().get_by_role('button', name='Cancel', exact=True).click()
            else:
                page.keyboard.press('Escape')
            dialog().wait_for(state='detached')
            settled()
            canceled = unchanged(before)
            assert canceled['modeRetries'] == before['modeRetries']
            before = state()
            count = start(mode_button(), before, 'mode')
            assert dialog().evaluate('(element) => element.getBoundingClientRect().left >= 0 && element.getBoundingClientRect().right <= innerWidth')
            dialog().screenshot(path=str(out / (prefix[1:] + '-confirm-' + str(width) + '.png')))
            after = confirm(before, 'mode', count)
            assert after['mode'] == 'rating_de'
            assert (after['movie'], after['series'], after['generic']) == ('G', 'TV-Y', 'PG')
            assert not after['knownMovie6Allowed'] and not after['knownSeries12Allowed']
            assert after['pairAttempts'] == pair_attempts

            # Actual reload shows stored invalid US values; it never maps them to ages.
            before = state()
            load(prefix)
            unchanged(before)
            assert movie().input_value() == 'G' and series().input_value() == 'TV-Y'
            pair_button().click()
            page.get_by_text('Select a German TMDB age for both movies and series.', exact=True).wait_for()
            assert state()['pairAttempts'] == before['pairAttempts']
            unchanged(before)

            if width == 1280:
                # Exercise the real server rejection as well as the UI validation.
                endpoint = args.base_url + prefix + '/api/users/' + args.profile + '/kids/ratings'
                bad = {'movieRating': '7', 'tvRating': '12'}
                before = state()
                challenge = context.request.put(endpoint, data=json.dumps(bad), headers={
                    'Content-Type': 'application/json', 'Origin': args.base_url, 'Sec-Fetch-Site': 'same-origin'})
                assert challenge.status == 403
                token = challenge.json()['parentIntent']
                assert token.startswith('prp_')
                unchanged(before)
                rejected = context.request.put(endpoint, data=json.dumps(bad), headers={
                    'Content-Type': 'application/json', 'Origin': args.base_url, 'Sec-Fetch-Site': 'same-origin',
                    'X-Parent-Intent': token, 'X-Account-Password': password})
                assert rejected.status == 400
                unchanged(before)

            movie().select_option('6')
            series().select_option('12')
            before = state()
            count = start(pair_button(), before, 'ratings')
            # The wire path is plural; fixture counters use the pair action name.
            count_before_confirm = count
            dialog().locator('input[type="password"]').fill(password)
            dialog().get_by_role('button', name='Confirm', exact=True).click()
            dialog().wait_for(state='detached')
            page.get_by_text('Ratings saved', exact=True).wait_for()
            settled()
            after = state()
            assert after['pairWrites'] == before['pairWrites'] + 1
            assert after['pairRetries'] == before['pairRetries'] + 1
            assert len(requests) == count_before_confirm + 2
            assert requests[-1]['body'] == requests[-2]['body']
            assert requests[-1]['intent'].startswith('prp_')
            assert json.loads(requests[-1]['body']) == {'movieRating': '6', 'tvRating': '12'}
            assert (after['mode'], after['movie'], after['series'], after['generic']) == ('rating_de', '6', '12', 'PG')
            assert after['knownMovie6Allowed'] and after['knownSeries12Allowed']
            load(prefix)
            unchanged(after)
            assert movie().input_value() == '6' and series().input_value() == '12'
            assert page.evaluate('Math.max(document.body.scrollWidth, document.documentElement.scrollWidth) <= innerWidth + 1')
            page.screenshot(path=str(out / (prefix[1:] + '-saved-' + str(width) + '.png')), full_page=True)

            # Returning to the legacy mode changes no existing rating value.
            select_mode('rating')
            before = state()
            pair_button().click()
            page.get_by_text('Confirm and save the selected mode first, then save its separate age limits.', exact=True).wait_for()
            assert state()['pairAttempts'] == before['pairAttempts']
            unchanged(before)
            count = start(mode_button(), before, 'mode')
            after = confirm(before, 'mode', count)
            assert (after['mode'], after['movie'], after['series'], after['generic']) == ('rating', '6', '12', 'PG')
            load(prefix)
            unchanged(after)
            assert not page.locator('#german-rating-note').is_visible()
            assert movie().locator('option:checked').text_content() == 'Saved value: 6'
            assert series().locator('option:checked').text_content() == 'Saved value: 12'

    # Missing separate limits remain unknown despite a populated legacy fallback.
    assert context.request.get(args.base_url + '/fixture/seed?kind=empty').status == 204
    load('/account')
    select_mode('rating_de')
    before = state()
    count = start(mode_button(), before, 'mode')
    final = confirm(before, 'mode', count)
    assert (final['mode'], final['movie'], final['series'], final['generic']) == ('rating_de', '', '', 'PG')
    assert not final['knownMovie6Allowed'] and not final['knownSeries12Allowed']
    pair_button().click()
    page.get_by_text('Select a German TMDB age for both movies and series.', exact=True).wait_for()
    unchanged(final)
    assert state()['pairAttempts'] == final['pairAttempts']
    assert final['modeWrites'] == 9 and final['pairWrites'] == 4
    assert final['unexpected'] == 0 and final['policyReads'] > 0
    assert not errors and not denied
    assert all(item['path'].endswith(('/kids/mode', '/kids/ratings')) for item in requests)
    (out / 'result.json').write_text(json.dumps({
        'fixture': 'actual-parent-cookie-pg-templates-assets', 'aliases': ['/admin', '/account'],
        'widths': [1280, 390], 'pageErrors': errors, 'blockedRequests': denied,
        'modeWrites': final['modeWrites'], 'pairWrites': final['pairWrites'],
        'unexpectedProfileCalls': final['unexpected'], 'readOnlyLivePolicyCalls': final['policyReads'],
        'explicitModeBeforePair': True, 'cancelAndEscapeWithoutWrites': True,
        'freshConfirmationForModeAndPair': True, 'oldUSValuesNotTranslated': True,
        'invalidNumericPairRejectedByActualServer': True, 'numericPairPersistsOnReload': True,
        'legacyModeSwitchPreservesFields': True, 'missingCapsNeverUseUSFallback': True, 'noAutomaticSave': True,
        'scope': 'Synthetic stored TMDB facts exercise current profile filters; no provider or media playback is requested.'
    }, indent=2) + '\n')
    browser.close()
