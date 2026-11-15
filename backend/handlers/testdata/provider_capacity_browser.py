"""Real settings template and cookie-authenticated routes; synthetic provider only."""
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
artifacts = Path(options.artifacts); artifacts.mkdir(parents=True, exist_ok=True)
errors = []; checks = []
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=options.chromium, headless=True, args=['--no-sandbox'])
    context = browser.new_context()
    base = options.base_url
    def info(): return context.request.get(base+'/fixture/info').json()
    assert info()['fixture'] == 'synthetic-provider-capacity'
    assert context.request.post(base+'/admin/api/live/provider-capacity', data={'sourceId':'xtream'}).status == 403
    assert info()['providerRequests'] == 0
    assert context.request.get(base+'/fixture/sign-in').status == 204
    def stored(): return context.request.get(base+'/admin/api/settings').json()['live']['sources'][0]['maxStreams']
    def prepare(page):
        page.goto(base+'/admin/settings')
        page.wait_for_function('typeof currentSettings !== "undefined" && currentSettings.live.sources.length===3')
        skip=page.locator('#adminWalkthroughSkip')
        if skip.is_visible(): skip.click()
        page.evaluate("setSettingsLevel('advanced')")
        page.evaluate("setSettingsGroup('sources');openSettingsSection('live.sources',{focus:false})")
    def open_report(page):
        page.get_by_role('button',name='Provider limit',exact=True).first.click()
        dialog=page.get_by_role('dialog',name='Provider connection limit')
        dialog.get_by_role('status').filter(has_text='provider reports').wait_for()
        return dialog
    for width in [320,375,390,414,768,1280]:
        page=context.new_page();page.set_viewport_size({'width':width,'height':1000})
        page.on('pageerror',lambda error:errors.append(str(error)))
        prepare(page)
        original=stored()
        dialog=open_report(page)
        assert stored()==original
        assert dialog.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
        assert page.evaluate('document.activeElement.textContent')=='Close'
        assert 'Provider-reported limit: 2' in dialog.inner_text()
        assert 'Provider-reported active connections: 1' in dialog.inner_text()
        assert 'snapshot' in dialog.inner_text()
        if width in [390,1280]:page.screenshot(path=str(artifacts/('provider-capacity-'+str(width)+'.png')),full_page=True)
        page.keyboard.press('Escape');dialog.wait_for(state='detached')
        assert stored()==original
        dialog=open_report(page)
        dialog.get_by_role('button',name='Use reported limit',exact=True).click();dialog.wait_for(state='detached')
        assert page.evaluate('currentSettings.live.sources[0].maxStreams')==2
        assert stored()==original
        with page.expect_response(lambda response:response.url.endswith('/admin/api/settings') and response.request.method=='PUT') as saved:
            page.evaluate('saveAllSettings()')
        assert saved.value.status==200
        assert stored()==2
        checks.append({'width':width,'real_template':True,'master_cookie_required':True,'checking_preserves_settings':True,'dismiss_preserves':True,'use_changes_form_only':True,'explicit_save_persists':True,'no_external_attribution_claim':True,'fits':True})
        page.close()
    page=context.new_page();page.set_viewport_size({'width':390,'height':1000});page.on('pageerror',lambda error:errors.append(str(error)))
    prepare(page)
    dialog=open_report(page)
    page.evaluate("currentSettings.live.sources[0].xtreamPassword='synthetic-unsaved-change'")
    dialog.get_by_role('status').filter(has_text='settings view changed').wait_for()
    assert dialog.get_by_role('button',name='Use reported limit').is_disabled()
    page.keyboard.press('Escape')
    prepare(page)
    # Malformed, failed and zero-limit responses preserve the current form and offer retry.
    pattern='**/admin/api/live/provider-capacity'
    for case in ['failure','zero','expired']:
        current=page.evaluate('currentSettings.live.sources[0].maxStreams')
        def respond(route):
            if case=='failure':route.fulfill(status=503,body='controlled fixture failure')
            else:
                data={'sourceId':'xtream','state':'available' if case=='expired' else 'unknown','message':'Synthetic report','reportedLimit':2 if case=='expired' else 0,'checkedAt':'2000-01-01T00:00:00Z','cacheExpiresAt':'2000-01-01T00:01:00Z'}
                route.fulfill(status=200,content_type='application/json',body=json.dumps(data))
        page.route(pattern,respond)
        page.get_by_role('button',name='Provider limit',exact=True).first.click()
        dialog=page.get_by_role('dialog',name='Provider connection limit')
        if case=='failure':dialog.get_by_role('status').filter(has_text='could not be checked').wait_for()
        else:dialog.get_by_role('status').filter(has_text='expired' if case=='expired' else 'Synthetic report').wait_for()
        assert dialog.get_by_role('button',name='Use reported limit').is_disabled()
        assert page.evaluate('currentSettings.live.sources[0].maxStreams')==current
        assert not dialog.get_by_role('button',name='Check provider limit').is_disabled()
        page.keyboard.press('Escape');dialog.wait_for(state='detached');page.unroute(pattern)
    assert stored()==2
    page.close();context.close();browser.close()
assert not errors,errors
result={'passed':True,'width_checks':checks,'changed_source_invalidates':True,'unknown_zero_never_means_unlimited':True,'failure_and_expiry_preserve_settings':True,'page_errors':errors,'synthetic_only':True,'full_application_login_tested':False,'native_ui_rendered':False}
(artifacts/'provider-capacity-browser-results.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'passed':True,'widths':len(checks)}))
