"""Actual cookie-authenticated settings and reports; synthetic providers only."""
import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

parser=argparse.ArgumentParser()
for key in ['base-url','chromium','artifacts']: parser.add_argument('--'+key,required=True)
opt=parser.parse_args();assert urlsplit(opt.base_url).hostname in ['127.0.0.1','localhost']
a=Path(opt.artifacts);a.mkdir(parents=True,exist_ok=True)
short_tmp=os.environ.get('MEDIASTORM_BROWSER_TMPDIR')
if short_tmp:
 short=Path(short_tmp);short.mkdir(parents=True,exist_ok=True)
 os.environ.update(TMPDIR=str(short),TMP=str(short),TEMP=str(short));tempfile.tempdir=str(short)
errors=[];widths=[]
with sync_playwright() as p:
 browser=p.chromium.launch(executable_path=opt.chromium,headless=True,args=['--no-sandbox'])
 context=browser.new_context(viewport={'width':390,'height':1000});base=opt.base_url
 assert context.request.get(base+'/fixture/runtime').status==403
 assert context.request.post(base+'/admin/api/live/provider-capacity',data={'sourceId':'xtream'}).status==403
 assert context.request.get(base+'/fixture/sign-in').status==204
 def runtime():
  value=context.request.get(base+'/fixture/runtime').json();assert value['fixture']=='synthetic-auto-provider-limits';return value
 def stored(): return context.request.get(base+'/admin/api/settings').json()['live']
 def save(page):
  with page.expect_response(lambda r:r.url.endswith('/admin/api/settings') and r.request.method=='PUT') as sent: page.evaluate('saveAllSettings()')
  assert sent.value.status==200
 def section(page,key):page.evaluate('(key)=>{setSettingsGroup(key==="live"?"all":"sources");openSettingsSection(key,{focus:false})}',key)
 def prepare(page):
  page.goto(base+'/admin/settings');page.wait_for_function('typeof currentSettings!=="undefined" && currentSettings.live.sources.length===3')
  skip=page.locator('#adminWalkthroughSkip')
  if skip.is_visible():skip.click()
  page.evaluate("setSettingsLevel('advanced')")
 def open_report(page):
  section(page,'live.sources')
  with page.expect_response(lambda r:r.url.endswith('/admin/api/live/provider-capacity') and r.request.method=='POST') as sent:page.get_by_role('button',name='Provider limit',exact=True).first.click()
  report=sent.value.json();dialog=page.get_by_role('dialog',name='Provider connection limit');dialog.wait_for(state='visible')
  dialog.get_by_role('status').filter(has_text=report['message']).wait_for()
  return dialog,report
 page=context.new_page();page.on('pageerror',lambda e:errors.append(str(e)));prepare(page)
 assert not stored().get('autoAdoptProviderLimits',False)
 assert runtime()['providerRequests']==0 and runtime()['limit']==3
 section(page,'live');toggle=page.locator('#live_autoAdoptProviderLimits')
 try:toggle.wait_for(state='attached',timeout=5000)
 except Exception:
  diagnostic=page.evaluate("""()=>({group:activeSettingsGroup,level:settingsLevel,selectedUser:Boolean(selectedUserId),selectedClient:Boolean(selectedClientId),schemaLiveFields:Object.keys(schema.live?.fields||{}),sections:Array.from(document.querySelectorAll('.section')).map(e=>e.id),autoInputs:Array.from(document.querySelectorAll('input')).map(e=>e.id).filter(x=>x.toLowerCase().includes('auto'))})""")
  (a/'missing-control-diagnostic.json').write_text(json.dumps(diagnostic,indent=2)+'\n')
  print(json.dumps(diagnostic));raise
 assert not toggle.is_checked();toggle.locator('..').click();assert toggle.is_checked()
 assert page.evaluate('currentSettings.live.autoAdoptProviderLimits') is True
 assert not stored().get('autoAdoptProviderLimits',False)
 assert runtime()['providerRequests']==0 and runtime()['limit']==3
 save(page);assert stored()['autoAdoptProviderLimits'] is True
 for _ in range(3):assert runtime()['providerRequests']==0 and runtime()['limit']==3
 dialog,report=open_report(page)
 assert report['automaticAdoptionEnabled'] is True and report['enforcementOrigin']=='provider_extra_cap'
 assert report['automaticLimit']==2 and report['enforcedLimit']==2 and report['configuredLimit']==3
 assert runtime()['providerRequests']==1 and runtime()['limit']==2 and runtime()['active']==0
 assert stored()['sources'][0]['maxStreams']==3
 dialog.get_by_text('Extra cap at check: 2',exact=False).wait_for()
 assert 'Running streams and recordings are retained.' in dialog.inner_text()
 dialog.screenshot(path=str(a/'auto-provider-cap-fresh-390.png'))
 page.keyboard.press('Escape');dialog.wait_for(state='detached')
 for width in [320,1280]:
  page.set_viewport_size({'width':width,'height':1000});dialog,report=open_report(page)
  assert report['enforcedLimit']==2 and runtime()['providerRequests']==1
  assert dialog.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
  widths.append(width);page.keyboard.press('Escape');dialog.wait_for(state='detached')
 # A test-only control expires the fixture's private synthetic observation.
 # Usage must fall back immediately without invoking the provider checker.
 assert context.request.post(base+'/fixture/expire').status==204
 assert runtime()['limit']==3 and runtime()['providerRequests']==1 and runtime()['enabled'] is True
 dialog,report=open_report(page)
 assert report['state']=='unavailable' and report['enforcementOrigin']=='manual'
 assert report['enforcedLimit']==3 and report.get('automaticLimit') is None
 assert runtime()['providerRequests']==2 and runtime()['limit']==3
 assert dialog.get_by_role('button',name='Use reported limit',exact=True).is_disabled()
 assert 'manual limit' in dialog.inner_text().lower()
 dialog.screenshot(path=str(a/'auto-provider-cap-fallback-1280.png'))
 page.keyboard.press('Escape');dialog.wait_for(state='detached')
 section(page,'live');toggle=page.locator('#live_autoAdoptProviderLimits');toggle.locator('..').click();assert not toggle.is_checked();save(page)
 assert stored()['autoAdoptProviderLimits'] is False
 assert runtime()['limit']==3 and runtime()['providerRequests']==2
 prepare(page);section(page,'live');assert not page.locator('#live_autoAdoptProviderLimits').is_checked()
 assert runtime()['providerRequests']==2
 page.close();context.close();browser.close()
assert not errors,errors
result={'passed':True,'actual_settings_template':True,'actual_master_cookie_required':True,'checkbox_changes_form_only_until_save':True,'opt_in_persists':True,'fresh_report_extra_cap':2,'manual_unchanged':3,'expired_observation_manual_fallback':True,'no_automatic_provider_calls':True,'explicit_provider_calls':2,'width_checks':widths,'page_errors':errors,'synthetic_expiry_injection':True,'real_login_or_stream_playback_tested':False}
(a/'auto-provider-limits-browser-results.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'provider_calls':2}))
