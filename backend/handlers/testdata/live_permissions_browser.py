"""Production templates and cookie-authenticated handlers; synthetic data only."""
import argparse, json
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

args=argparse.ArgumentParser()
args.add_argument('--base-url',required=True);args.add_argument('--chromium',required=True);args.add_argument('--artifacts',required=True)
options=args.parse_args();origin=urlsplit(options.base_url)
assert origin.scheme=='http' and origin.hostname in ['127.0.0.1','localhost']
artifacts=Path(options.artifacts);artifacts.mkdir(parents=True,exist_ok=True)
password='synthetic-parent-confirmation'
errors=[];checks=[]
with sync_playwright() as p:
    browser=p.chromium.launch(executable_path=options.chromium,headless=True,args=['--no-sandbox'])
    context=browser.new_context()
    assert context.request.get(options.base_url+'/fixture/sign-in').status==204
    info=context.request.get(options.base_url+'/fixture/info').json();assert info['fixture']=='synthetic-live-permissions'
    def profiles():
        response=context.request.get(options.base_url+'/account/api/profiles');assert response.status==200
        return response.json()
    def current():return next(profile for profile in profiles()if profile['id']==info['adult'])
    assert all(profile['id']!=info['foreign']for profile in profiles())
    denied=context.request.put(options.base_url+'/account/api/profiles/live-permissions?profileId='+info['foreign'],headers={'X-Account-Password':password},data={'allowLiveTakeover':True,'allowRecordingInterruption':True})
    assert denied.status==404
    for route in ['/account/users','/account']:
        for width in [320,375,390,414,768,1280]:
            reset=context.request.put(options.base_url+'/account/api/profiles/live-permissions?profileId='+info['adult'],headers={'X-Account-Password':password},data={'allowLiveTakeover':False,'allowRecordingInterruption':False});assert reset.status==200
            page=context.new_page();page.set_viewport_size({'width':width,'height':1000})
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto(options.base_url+route)
            def open_rights(profile_id):
                card=page.locator('[data-profile-id="'+profile_id+'"]')
                card.first.wait_for(state='attached')
                collapsed=page.locator('.household-card.collapsed')
                if collapsed.count():collapsed.first.locator('.household-summary').click()
                card.get_by_role('button',name='IPTV rights',exact=True).first.click()
            open_rights(info['adult'])
            dialog=page.get_by_role('dialog',name='IPTV takeover rights')
            assert dialog.is_visible()
            assert page.evaluate('document.activeElement.textContent')=='Close'
            assert not dialog.locator('[name=allowLiveTakeover]').is_checked()
            assert not dialog.locator('[name=allowRecordingInterruption]').is_checked()
            assert dialog.evaluate('(el)=>el.getBoundingClientRect().left>=0 && el.getBoundingClientRect().right<=innerWidth')
            assert dialog.evaluate('(el)=>Math.abs((el.getBoundingClientRect().left+el.getBoundingClientRect().right)/2-innerWidth/2)<1')
            dialog.locator('[name=allowLiveTakeover]').check()
            dialog.get_by_role('button',name='Save IPTV rights').click()
            prompt=page.get_by_role('dialog',name='Confirm account password');prompt.wait_for()
            prompt.get_by_role('button',name='Cancel',exact=True).click()
            dialog.get_by_role('status').filter(has_text='could not be saved').wait_for()
            assert current()['allowLiveTakeover'] is False
            dialog.get_by_role('button',name='Save IPTV rights').click();prompt.wait_for()
            prompt.locator('input[type=password]').fill(password);prompt.get_by_role('button',name='Confirm',exact=True).click()
            dialog.wait_for(state='detached')
            page.wait_for_function('''id=>Array.from(document.querySelectorAll('[data-profile-id]')).some(el=>el.dataset.profileId===id)''',arg=info['adult'])
            assert current()['allowLiveTakeover'] is True and current()['allowRecordingInterruption'] is False
            # A failed request keeps unconfirmed choices and exposes a real retry.
            page.reload();open_rights(info['adult'])
            dialog=page.get_by_role('dialog',name='IPTV takeover rights');assert dialog.locator('[name=allowLiveTakeover]').is_checked()
            if width in [390,1280]:page.screenshot(path=str(artifacts/('iptv-rights-'+('users'if route.endswith('users')else'profiles')+'-'+str(width)+'.png')),full_page=True)
            dialog.locator('[name=allowLiveTakeover]').uncheck();dialog.locator('[name=allowRecordingInterruption]').check()
            pattern='**/account/api/profiles/live-permissions?*'
            page.route(pattern,lambda route:route.fulfill(status=503,content_type='application/json',body='{"message":"Synthetic controlled failure"}'))
            dialog.get_by_role('button',name='Save IPTV rights').click()
            dialog.get_by_role('status').filter(has_text='could not be saved').wait_for()
            assert current()['allowRecordingInterruption'] is False
            assert dialog.locator('[name=allowRecordingInterruption]').is_checked()
            page.unroute(pattern)
            dialog.get_by_role('button',name='Save IPTV rights').click();prompt=page.get_by_role('dialog',name='Confirm account password');prompt.wait_for()
            prompt.locator('input[type=password]').fill(password);prompt.get_by_role('button',name='Confirm',exact=True).click();dialog.wait_for(state='detached')
            assert current()['allowLiveTakeover'] is False and current()['allowRecordingInterruption'] is True
            page.reload();open_rights(info['child'])
            dialog=page.get_by_role('dialog',name='IPTV takeover rights')
            assert dialog.get_by_role('button',name='Save IPTV rights').is_disabled()
            assert dialog.locator('[name=allowLiveTakeover]').is_disabled()
            page.keyboard.press('Escape');dialog.wait_for(state='detached')
            checks.append({'template':route,'width':width,'real_cookie_auth':True,'fresh_parent_confirmation':True,'dismiss_preserves':True,'independent_persisted_rights':True,'failure_retry':True,'child_rights_disabled':True,'dialog_fits':True})
            page.close()
    context.close();browser.close()
assert not errors,errors
result={'passed':True,'checks':checks,'page_errors':0,'synthetic_only':True,'production_templates_and_handlers':True,'fixture_cookie_issued_by_test_only':True,'full_application_login_tested_here':False,'actual_provider_or_private_playlist_used':False,'native_ui_rendered':False}
(artifacts/'live-permissions-browser-results.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'passed':True,'production_templates':2,'widths':6}))
