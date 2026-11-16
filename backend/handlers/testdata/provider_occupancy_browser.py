"""Real static UI and serialized reports; synthetic cookie/provider/cache only."""
import argparse
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

parser=argparse.ArgumentParser()
for key in ['base-url','chromium','artifacts']: parser.add_argument('--'+key,required=True)
opt=parser.parse_args();assert urlsplit(opt.base_url).hostname in ['127.0.0.1','localhost']
a=Path(opt.artifacts);a.mkdir(parents=True,exist_ok=True)
errors=[]
def moment(value): return datetime.fromisoformat(value.replace('Z','+00:00'))
with sync_playwright() as p:
 browser=p.chromium.launch(executable_path=opt.chromium,headless=True,args=['--no-sandbox'])
 context=browser.new_context(viewport={'width':390,'height':1000});base=opt.base_url
 assert context.request.post(base+'/admin/api/live/provider-capacity',data={'sourceId':'xtream'}).status==403
 assert context.request.get(base+'/fixture/sign-in').status==204
 def runtime():
  value=context.request.get(base+'/fixture/runtime').json();assert value['fixture']=='synthetic-auto-provider-limits';return value
 page=context.new_page();page.on('pageerror',lambda e:errors.append(str(e)));page.clock.install()
 page.goto(base+'/admin/settings');page.wait_for_function('typeof currentSettings!=="undefined" && currentSettings.live.sources.length===3')
 skip=page.locator('#adminWalkthroughSkip')
 if skip.is_visible():skip.click()
 page.evaluate("setSettingsLevel('advanced');setSettingsGroup('all');openSettingsSection('live',{focus:false})")
 toggle=page.locator('#live_autoAdoptProviderLimits');assert not toggle.is_checked();toggle.locator('..').click()
 with page.expect_response(lambda r:r.url.endswith('/admin/api/settings') and r.request.method=='PUT') as saved:page.evaluate('saveAllSettings()')
 assert saved.value.status==200 and runtime()['providerRequests']==0
 def open_report():
  page.evaluate("setSettingsGroup('sources');openSettingsSection('live.sources',{focus:false})")
  with page.expect_response(lambda r:r.url.endswith('/admin/api/live/provider-capacity') and r.request.method=='POST') as sent:page.get_by_role('button',name='Provider limit',exact=True).first.click()
  report=sent.value.json();dialog=page.get_by_role('dialog',name='Provider connection limit');dialog.wait_for(state='visible')
  dialog.get_by_role('status').filter(has_text=report['message']).wait_for()
  assert report['externalUsage']['state']=='unknown' and set(report['externalUsage'])=={'state','reason'}
  dialog.get_by_text('Usage outside Mediastorm: Unknown',exact=True).wait_for()
  assert 'Provider response check completed:' in dialog.inner_text()
  assert 'Local reservation snapshot completed:' in dialog.inner_text()
  assert 'not a provider measurement time' in dialog.inner_text()
  return dialog,report
 dialog,first=open_report()
 assert first['externalUsage']['reason']=='no_common_trusted_measurement' and first['providerActiveConnections']==1
 assert first['automaticLimit']==2 and first['enforcedLimit']==2 and first['configuredLimit']==3
 assert moment(first['localSnapshotCompletedAt'])>=moment(first['checkedAt'])
 assert runtime()['providerRequests']==1
 dialog.screenshot(path=str(a/'occupancy-unknown-fresh-390.png'))
 page.keyboard.press('Escape');dialog.wait_for(state='detached')
 dialog,second=open_report()
 assert second['cached'] is True and second['checkedAt']==first['checkedAt'] and second['cacheExpiresAt']==first['cacheExpiresAt']
 assert moment(second['localSnapshotCompletedAt'])>moment(first['localSnapshotCompletedAt'])
 assert runtime()['providerRequests']==1
 for width in [320,1280]:
  page.set_viewport_size({'width':width,'height':1000})
  assert dialog.evaluate('(e)=>e.getBoundingClientRect().left>=0&&e.getBoundingClientRect().right<=innerWidth')
 # Advance only the browser clock: old provider values remain explicitly
 # historical and unknown; no network request is used to refresh the report.
 page.clock.fast_forward(65000)
 dialog.get_by_text('Provider-reported active connections: 1 (expired snapshot)',exact=True).wait_for()
 assert dialog.get_by_role('button',name='Use reported limit',exact=True).is_disabled()
 assert 'Usage outside Mediastorm: Unknown' in dialog.inner_text()
 assert runtime()['providerRequests']==1
 page.keyboard.press('Escape');dialog.wait_for(state='detached')
 # The fixture's explicit expiry control changes only its private synthetic
 # observation. The following deliberate check returns a real failed report.
 assert context.request.post(base+'/fixture/expire').status==204
 assert runtime()['limit']==3 and runtime()['providerRequests']==1
 dialog,failed=open_report()
 assert failed['externalUsage']['reason']=='provider_count_unavailable'
 assert 'providerActiveConnections' not in failed and failed['state']=='unavailable'
 assert failed['enforcedLimit']==3 and failed.get('automaticLimit') is None
 assert 'Provider-reported active connections: Unknown' in dialog.inner_text()
 assert dialog.get_by_role('button',name='Use reported limit',exact=True).is_disabled()
 assert runtime()['providerRequests']==2
 dialog.screenshot(path=str(a/'occupancy-unknown-unavailable-1280.png'))
 page.close();context.close();browser.close()
assert not errors,errors
result={'passed':True,'actual_template_cookie_and_handler':True,'external_usage_always_unknown':True,'no_numeric_external_count_or_range':True,'cached_provider_time_retained':True,'local_time_resampled':True,'server_completion_not_provider_measurement':True,'browser_only_expiry_labels_historical':True,'unavailable_count_explicit_unknown':True,'manual_and_auto_caps_preserved':True,'explicit_provider_http':2,'automatic_provider_http':0,'page_errors':errors,'synthetic_expiry_injection':True,'real_provider_or_login_or_streams':False}
(a/'provider-occupancy-browser-results.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'provider_http':2}))
