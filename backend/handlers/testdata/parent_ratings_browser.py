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
path = prefix + '/kids-settings?profileId=' + args.profile
mode_path = prefix + '/api/users/' + args.profile + '/kids/ratings'
pattern = '**' + mode_path
password = 'synthetic-parent-password'

with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox', '--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
    context = browser.new_context(viewport={'width': 1280, 'height': 900})
    errors, denied, scalar_requests, pair_bodies = [], [], [], []

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

    def record_request(request):
        target = urlparse(request.url).path
        if target in [prefix + '/api/users/' + args.profile + '/kids/rating' + suffix for suffix in ['', '/movie', '/tv']]:
            scalar_requests.append(request.method)
        if target == mode_path and request.method == 'PUT':
            pair_bodies.append(request.post_data)

    page.on('request', record_request)
    assert context.request.get(args.base_url + '/fixture/sign-in').status == 204
    state = lambda: context.request.get(args.base_url + '/fixture/state').json()
    dialog = lambda: page.get_by_role('dialog', name='Confirm account password', exact=True)
    button = lambda: page.locator('#save-rating-pair')
    movie = lambda: page.locator('#movieRating')
    tv = lambda: page.locator('#tvRating')

    def loaded(kind='normal'):
        assert context.request.get(args.base_url + '/fixture/seed?kind=' + kind).status == 204
        page.goto(args.base_url + path)
        button().wait_for()
        page.wait_for_function('!document.querySelector("#save-rating-pair").disabled')
        assert page.locator('script[src*="account-parent-auth-v1.js"]').count() == 1
        assert page.evaluate('typeof window.parentRatingFetch') == 'function'

    def start(movie_value=None, tv_value=None):
        before = state()
        selected_movie = movie_value if movie_value is not None else ('PG' if before['movie'] != 'PG' else 'G')
        selected_tv = tv_value if tv_value is not None else ('TV-G' if before['tv'] != 'TV-G' else 'TV-Y')
        movie().select_option(selected_movie)
        tv().select_option(selected_tv)
        button().click()
        dialog().wait_for()
        assert dialog().count() == 1 and state()['writes'] == before['writes']
        return before, selected_movie, selected_tv

    def confirm():
        dialog().locator('input[type="password"]').fill(password)
        dialog().get_by_role('button', name='Confirm', exact=True).click()
        dialog().wait_for(state='detached')

    def no_retry(before):
        page.wait_for_timeout(100)
        after = state()
        assert after['writes'] == before['writes']
        assert after['retries'] == before['retries']
        assert after['kids'] is True

    for width in [1280,390]:
        page.set_viewport_size({'width':width,'height':900 if width==1280 else 844})
        loaded()
        before,_,_ = start()
        if width == 1280:
            dialog().get_by_role('button',name='Cancel',exact=True).click()
        else:
            page.keyboard.press('Escape')
        dialog().wait_for(state='detached')
        no_retry(before)
        page.wait_for_function('!document.querySelector("#save-rating-pair").disabled')
        bodies = len(pair_bodies)
        before,selected_movie,selected_tv = start()
        assert dialog().evaluate('(element)=>element.getBoundingClientRect().left>=0&&element.getBoundingClientRect().right<=innerWidth')
        dialog().screenshot(path=str(out / ('confirm-' + str(width) + '.png')))
        confirm()
        page.get_by_text('Ratings saved',exact=True).wait_for()
        after = state()
        assert after['writes'] == before['writes'] + 1 and after['retries'] == before['retries'] + 1
        assert after['movie'] == selected_movie and after['tv'] == selected_tv and after['generic'] == before['generic']
        assert after['kids'] is True and after['mode'] == before['mode'] == 'rating'
        assert len(pair_bodies) == bodies + 2 and pair_bodies[-1] == pair_bodies[-2]
        assert json.loads(pair_bodies[-1]) == {'movieRating':selected_movie,'tvRating':selected_tv}
        assert password not in page.content()

    loaded('empty')
    assert movie().input_value() == '' and tv().input_value() == '' and state()['generic'] == 'PG'
    before,_,_ = start('','')
    confirm()
    page.get_by_text('Ratings saved',exact=True).wait_for()
    after = state()
    assert after['movie'] == '' and after['tv'] == '' and after['generic'] == 'PG'
    assert after['writes'] == before['writes'] + 1
    assert json.loads(pair_bodies[-1]) == {'movieRating':'','tvRating':''}

    loaded('unknown')
    before = state()
    assert movie().input_value() == before['movie'] and tv().input_value() == before['tv']
    assert movie().locator('option:checked').text_content() == 'Saved value: ' + before['movie']
    start(before['movie'],before['tv'])
    confirm()
    page.get_by_text('Ratings saved',exact=True).wait_for()
    after = state()
    assert after['movie'] == before['movie'] and after['tv'] == before['tv'] and after['generic'] == before['generic']
    assert after['writes'] == before['writes'] + 1

    loaded()
    before,_,_ = start()
    assert context.request.get(args.base_url + '/fixture/rotate').status == 204
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.',exact=True).wait_for()
    assert state()['writes'] == before['writes']

    loaded()
    before,_,_ = start()
    assert context.request.get(args.base_url + '/fixture/mutate-target').status == 204
    confirm()
    page.get_by_text('Confirmation expired or the request changed. Save again.',exact=True).wait_for()
    assert state()['writes'] == before['writes']

    for field,value in [('movieRating','NC-17'),('tvRating','TV-MA')]:
        loaded()
        before,_,_ = start()
        page.evaluate("args=>{const field=document.getElementById(args[0]);field.value=args[1];field.dispatchEvent(new Event('change',{bubbles:true}))}",[field,value])
        dialog().wait_for(state='detached')
        no_retry(before)

    loaded()
    before,_,_ = start()
    page.evaluate("history.replaceState({},'', '?profileId=different-profile')")
    confirm()
    no_retry(before)

    loaded()
    before,_,_ = start()
    page.evaluate("const form=document.querySelector('#rating-section');form.replaceWith(form.cloneNode(true))")
    confirm()
    no_retry(before)

    loaded()
    before,_,_ = start()
    page.evaluate("const radio=document.querySelector('input[name=kidsMode][value=content_list]');radio.checked=true;radio.dispatchEvent(new Event('change',{bubbles:true}))")
    dialog().wait_for(state='detached')
    no_retry(before)

    # Seven earlier unclaimed canceled intents plus this eighth fit the real
    # shared actor budget. Do not reset the registry or extend server deadlines.
    loaded()
    before,_,_ = start()
    dialog().wait_for(state='detached',timeout=33000)
    no_retry(before)

    loaded()
    before,_,_ = start()
    page.evaluate("profile.kidsMaxRating='changed-local-generic'")
    confirm()
    no_retry(before)

    loaded()
    before,_,_ = start()
    page.goto(args.base_url + '/fixture/away')
    no_retry(before)

    loaded()
    held = []

    def hold_first(route):
        held.append((route,route.fetch()))

    page.route(pattern,hold_first)
    before = state()
    movie().select_option('PG')
    tv().select_option('TV-G')
    button().click()
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held
    page.goto(args.base_url + '/fixture/away')
    route,response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass
    page.unroute(pattern,hold_first)
    assert dialog().count() == 0
    no_retry(before)

    loaded()
    held = []

    def hold_success(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            held.append((route,response))
        else:
            route.fulfill(response=response)

    page.route(pattern,hold_success)
    before,selected_movie,selected_tv = start()
    confirm()
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(20)
    assert held and state()['writes'] == before['writes'] + 1
    movie().select_option('NC-17')
    tv().select_option('TV-MA')
    route,response = held.pop()
    try:
        route.fulfill(response=response)
    except Exception:
        pass
    page.unroute(pattern,hold_success)
    page.wait_for_timeout(100)
    assert state()['movie'] == selected_movie and state()['tv'] == selected_tv
    assert movie().input_value() == 'NC-17' and tv().input_value() == 'TV-MA'
    assert page.get_by_text('Ratings saved',exact=True).count() == 0

    loaded()

    def unknown_after_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=503,content_type='application/json',body=json.dumps({'code':'parent_confirmation_outcome_unknown','error':'PRIVATE_REPLY_CANARY'}))
        else:
            route.fulfill(response=response)

    page.route(pattern,unknown_after_commit)
    before,_,_ = start()
    confirm()
    page.get_by_text('The result is not known. Reload settings before trying again.',exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1 and button().is_disabled()
    attempts = state()['attempts']
    movie().select_option('R')
    assert button().is_disabled() and state()['attempts'] == attempts and 'PRIVATE_REPLY_CANARY' not in page.content()
    page.unroute(pattern,unknown_after_commit)

    loaded()

    def malformed_after_commit(route):
        response = route.fetch()
        if route.request.headers.get('x-parent-intent') and response.status == 200:
            route.fulfill(status=200,content_type='application/json',body=json.dumps({'id':args.profile,'kidsMaxMovieRating':None,'kidsMaxTVRating':'TV-G','kidsMaxRating':'PG'}))
        else:
            route.fulfill(response=response)

    page.route(pattern,malformed_after_commit)
    before,_,_ = start()
    confirm()
    page.get_by_text('The result could not be confirmed. Reload settings before trying again.',exact=True).wait_for()
    assert state()['writes'] == before['writes'] + 1 and button().is_disabled()
    page.unroute(pattern,malformed_after_commit)

    loaded()
    before,_,_ = start()
    assert context.request.get(args.base_url + '/fixture/retire-actor').status == 204
    confirm()
    page.wait_for_timeout(100)
    assert state()['writes'] == before['writes']
    assert page.get_by_text('Ratings saved',exact=True).count() == 0
    assert state()['scalarCalls'] == 0 and not scalar_requests
    assert not errors and not denied
    (out / 'result.json').write_text(json.dumps({
        'fixture':'actual-cookie-template-asset-own-schema-pg-rating-pair','role':args.role,'widths':[1280,390],
        'pageErrors':errors,'blockedPageRequests':denied,'scalarWebRequests':scalar_requests,'freshMasterProof':args.role=='master',
        'onePairHTTPActionOnePasswordDialog':True,'cancelEscapeNoRetry':True,'bothEmptyPreservesGenericFallback':True,
        'unknownStoredStringsPreserved':True,'identicalPairStillUpdates':True,'cookieAndConcurrentTargetChangeDenied':True,
        'originalMovieTVFormProfileModePageBound':True,'lateFirstChallengeIgnored':True,'localPromptWaitBounded':True,
        'lateCommittedPairDoesNotReplaceNewSelectionsOrPublishSuccess':True,'unknownAndMalformedRequireReload':True,
        'noScalarFallbackRequests':True,'actorRetirementDenied':True,
        'unknownFixtureScope':'HTTP reply replaced after known actual pair commit; real datastore COMMIT uncertainty is separately tested.',
        'policyScope':'No new enum/regional/playback rule; no playback process was exercised.'
    },indent=2) + '\n')
    browser.close()
