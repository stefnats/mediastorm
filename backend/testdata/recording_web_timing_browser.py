"""Actual template/cookie/PIN/controller/FD inspection; synthetic local targets."""
import argparse,copy,datetime,email.utils,hashlib,json,time,zipfile
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from playwright.sync_api import sync_playwright
p=argparse.ArgumentParser();p.add_argument('--base-url',required=True);p.add_argument('--chromium',required=True);p.add_argument('--artifacts',required=True);a=p.parse_args()
origin=urlsplit(a.base_url);assert origin.hostname in ('127.0.0.1','localhost')
art=Path(a.artifacts);art.mkdir(parents=True,exist_ok=True)
errors=[];checks=[];requests=[];denied=[]
with sync_playwright() as driver:
 browser=driver.chromium.launch(executable_path=a.chromium,headless=True,args=['--no-sandbox'])
 context=browser.new_context(accept_downloads=True)
 def only_fixture(route):
  target=urlsplit(route.request.url)
  if target.scheme in ('about','data') or (target.scheme,target.netloc)==(origin.scheme,origin.netloc):route.continue_()
  else:denied.append({'scheme':target.scheme,'offOrigin':True});route.abort()
 context.route('**/*',only_fixture)
 info=context.request.get(a.base_url+'/fixture/info').json()
 def state():return context.request.get(a.base_url+'/fixture/timing').json()
 def wait(condition):
  deadline=time.monotonic()+5
  while not condition():
   assert time.monotonic()<deadline,'Fixture barrier timed out'
   page.wait_for_timeout(10)
 def observe(req):
  if '/timing/' not in urlsplit(req.url).path:return
  parsed=urlsplit(req.url);assert not parsed.query and req.method=='POST'
  fields=parse_qs(req.post_data or '',keep_blank_values=True)
  assert set(fields)=={'profileId','profileUnlock'} and all(len(v)==1 for v in fields.values())
  assert '1234' not in fields['profileUnlock']
  requests.append({'method':'POST','bodyOnly':True,'singleOpaque':True,'operation':parsed.path.rsplit('/',1)[1]})
 for role in ['account','admin']:
  assert context.request.get(a.base_url+'/fixture/sign-in?role='+role).status==204
  for width in [390,1280]:
   assert context.request.get(a.base_url+'/fixture/timing-mode?value=real').status==204
   page=context.new_page();page.set_viewport_size({'width':width,'height':1100});page.on('pageerror',lambda e:errors.append(str(e)));page.on('request',observe)
   before=state();page.goto(a.base_url+'/'+role+'/recordings');page.locator('#recordingProfile').select_option(info['locked'])
   job='job-'+info['locked'];read=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=read]');inspect=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');part=page.locator('[data-recording-part="'+job+'"]');read.wait_for()
   assert part.locator('option').evaluate_all('(items)=>items.map(i=>i.value)')==['1','3']
   assert state()['reads']==before['reads'] and state()['refreshes']==before['refreshes'],'Render/profile switch caused timing work'
   read.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible');assert state()['reads']==before['reads'] and state()['refreshes']==before['refreshes']
   dialog.locator('[data-export-pin]').fill('1234');dialog.get_by_role('button',name='Unlock and view estimate',exact=True).click()
   page.locator('#recordingExportStatus').get_by_text('No current estimate is stored for this part.',exact=False).wait_for()
   assert state()['reads']==before['reads']+1 and state()['refreshes']==before['refreshes']
   inspect.click();page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length: 2.0 seconds.',exact=False).wait_for();assert state()['refreshes']==before['refreshes']+1
   read.click();page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length: 2.0 seconds.',exact=False).wait_for();assert state()['refreshes']==before['refreshes']+1
   part.select_option('3');assert 'selection changed' in page.locator('#recordingExportStatus').inner_text();assert state()['refreshes']==before['refreshes']+1
   inspect.click();page.locator('#recordingExportStatus').get_by_text('Part 3 estimated part length: 3.0 seconds.',exact=False).wait_for();assert state()['refreshes']==before['refreshes']+2
   # The original ZIP path still streams original independent bytes after timing.
   with page.expect_download() as event:page.locator('[data-recording-export="'+job+'"]').click()
   download=event.value;assert download.failure() is None
   with zipfile.ZipFile(download.path()) as archive:
    assert set(archive.namelist())=={'part-000001.ts','part-000003.ts','manifest.json'}
    assert all(entry.compress_type==zipfile.ZIP_STORED for entry in archive.infolist())
    hashes=state()['hashes']
    for i in ['1','3']:assert hashlib.sha256(archive.read('part-'+i.zfill(6)+'.ts')).hexdigest()==hashes[i]
   part.select_option('1');read.click();page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length:',exact=False).wait_for()
   assert 'combined timeline' in page.locator('#recordingExportStatus').inner_text()
   assert page.locator('#recordingExportPINInput').input_value()=='' and not page.evaluate('Object.keys(localStorage).some(k=>/unlock|pin/i.test(k))')
   assert not urlsplit(page.url).query
   if role=='account':page.screenshot(path=str(art/('timing-'+str(width)+'.png')),full_page=False)
   checks.append({'role':role,'width':width,'noAutoProbe':True,'actualPart1FD':True,'actualPart3FD':True,'cacheReadOnly':True,'originalZIPBytes':True,'bodyOnlyUnlock':True})
   page.close()
 # A late non-abortable old ZIP denial cannot replace the newer timing PIN intent.
 assert context.request.get(a.base_url+'/fixture/sign-in?role=account').status==204
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['locked']);job='job-'+info['locked'];second='second-'+info['locked']
 first=page.locator('[data-recording-export="'+job+'"]');first.wait_for();assert context.request.get(a.base_url+'/fixture/hold?kind=denial').status==204
 first.click();wait(lambda:context.request.get(a.base_url+'/fixture/info').json()['waitingDenied'])
 page.locator('[data-recording-timing="'+second+'"][data-timing-operation=read]').click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible')
 assert context.request.get(a.base_url+'/fixture/release').status==204;page.wait_for_timeout(100)
 assert dialog.get_by_role('button',name='Unlock and view estimate',exact=True).is_visible()
 before=state();dialog.locator('[data-export-pin]').fill('1234');dialog.get_by_role('button',name='Unlock and view estimate',exact=True).click();page.locator('#recordingExportStatus').get_by_text('No current estimate is stored',exact=False).wait_for();assert state()['refreshes']==before['refreshes']
 checks.append({'lateZIPCannotReplaceTimingPIN':True});page.close()
 # Abort an admitted actual subprocess via the original fetch request context.
 assert context.request.get(a.base_url+'/fixture/timing-mode?value=slow').status==204
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['open']);job='job-'+info['open'];button=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');button.wait_for();button.click();wait(lambda:state()['started'])
 before_cancel=state();page.locator('[data-recording-timing-cancel]').first.click();page.locator('#recordingExportStatus').get_by_text('request cancelled',exact=False).wait_for();wait(lambda:state()['finished']==before_cancel['finished']+1 and not state()['alive'])
 assert 'estimated part length:' not in page.locator('#recordingExportStatus').inner_text()
 checks.append({'originalFetchCancellation':True,'noLateEstimate':True});page.close()
 assert context.request.get(a.base_url+'/fixture/timing-mode?value=real').status==204
 # Each selection/list/profile retirement aborts the original admitted process,
 # without automatically starting an input for the replacement selection.
 for retirement in ['part','profile','list']:
  assert context.request.get(a.base_url+'/fixture/timing-mode?value=slow').status==204
  page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['open']);job='job-'+info['open']
  inspect=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');inspect.wait_for();before=state();inspect.click();wait(lambda:state()['started'])
  if retirement=='part':page.locator('[data-recording-part="'+job+'"]').select_option('3')
  elif retirement=='profile':page.locator('#recordingProfile').select_option(info['locked'])
  else:page.get_by_role('button',name='Refresh',exact=True).click()
  wait(lambda:state()['finished']==before['finished']+1 and not state()['alive'])
  assert state()['refreshes']==before['refreshes']+1 and 'estimated part length:' not in page.locator('#recordingExportStatus').inner_text()
  checks.append({'retiredOriginal':retirement,'joinedActualChild':True,'noNewAutomaticInput':True});page.close()
 assert context.request.get(a.base_url+'/fixture/timing-mode?value=invalid').status==204
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['open']);job='job-'+info['open'];inspect=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');inspect.wait_for();inspect.click();page.locator('#recordingExportStatus').get_by_text('No duration has been inferred.',exact=False).wait_for();checks.append({'actualUnavailableProbeIsUnknown':True});page.close()
 assert context.request.get(a.base_url+'/fixture/timing-mode?value=real').status==204
 # Real successful response is altered only in this explicit transport decoder fixture.
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['open']);job='job-'+info['open']
 inspect=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');read=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=read]');inspect.wait_for();inspect.click();page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length:',exact=False).wait_for()
 selected={'mode':'wrong part'}
 def changed_response(route):
  response=route.fetch(max_redirects=0);assert response.status==200
  data=response.json();assert data['state']=='container_estimate'
  mode=selected['mode'];headers={k:v for k,v in response.headers.items() if k.lower() not in ('content-length','content-encoding','transfer-encoding')}
  if mode=='wrong part':data['partIndex']=3
  elif mode=='wrong recording':data['recordingId']='foreign-recording'
  elif mode=='extra method':data['method']='POST'
  elif mode=='extra raw error':data['error']='PRIVATE_NOT_DISPLAYED'
  elif mode=='zero':data['containerDurationMs']=0
  elif mode=='fraction':data['containerDurationMs']=1.5
  elif mode=='unsafe integer':data['containerDurationMs']=9007199254740992
  elif mode=='state':data['state']='complete'
  elif mode=='missing time':del data['observedAt']
  elif mode=='malformed time':data['observedAt']='2026-11-16Z'
  elif mode=='calendar':data['observedAt']='2026-02-30T12:00:00Z';data['expiresAt']='2026-03-02T12:15:00Z'
  elif mode=='future':
   at=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)
   data['observedAt']=at.isoformat().replace('+00:00','Z');data['expiresAt']=(at+datetime.timedelta(minutes=15)).isoformat().replace('+00:00','Z')
  elif mode=='expired':
   at=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(minutes=20)
   data['observedAt']=at.isoformat().replace('+00:00','Z');data['expiresAt']=(at+datetime.timedelta(minutes=15)).isoformat().replace('+00:00','Z')
  elif mode=='missing Date':headers.pop('date',None)
  elif mode=='backward Date':headers['date']=email.utils.formatdate(time.time()-3600,usegmt=True)
  elif mode=='busy':route.fulfill(status=429,body='fixed synthetic busy',content_type='text/plain');return
  route.fulfill(status=200,body=json.dumps(data),headers=headers)
 page.route(a.base_url+'/account/api/live/recordings/'+job+'/parts/1/timing/read',changed_response)
 for mode in ['wrong part','wrong recording','extra method','extra raw error','zero','fraction','unsafe integer','state','missing time','malformed time','calendar','future','expired','busy','missing Date','backward Date']:
  selected['mode']=mode;before=state();read.click()
  text='Estimate expired.' if mode=='expired' else 'Inspection is busy.' if mode=='busy' else 'Part estimate unavailable.'
  page.locator('#recordingExportStatus').get_by_text(text,exact=False).wait_for()
  assert state()['refreshes']==before['refreshes'] and 'PRIVATE_' not in page.locator('#recordingExportStatus').inner_text()
 checks.append({'actualResponseDecoderTransportControls':16,'strictCalendarAndFuture':True,'expiredNoNumericDisplay':True,'noAutomaticRefresh':True});page.close()
 # Actual server HTTP-Date, not the client wall clock, bounds PIN and timing.
 for shift in [-3600000,3600000]:
  assert context.request.get(a.base_url+'/fixture/timing-mode?value=real').status==204
  page=context.new_page();page.add_init_script('const actualNow = Date.now.bind(Date); Date.now = () => actualNow() + '+str(shift)+';');page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['locked']);job='job-'+info['locked']
  inspect=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');inspect.wait_for();inspect.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible');dialog.locator('[data-export-pin]').fill('1234');dialog.get_by_role('button',name='Unlock and inspect',exact=True).click()
  page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length:',exact=False).wait_for()
  with page.expect_download() as event:page.locator('[data-recording-export="'+job+'"]').click()
  assert event.value.failure() is None
  # A subsequent arbitrary client wall-clock reset cannot renew/expire the receipt.
  page.evaluate('Date.now = () => 0')
  page.locator('[data-recording-timing="'+job+'"][data-timing-operation=read]').click();page.locator('#recordingExportStatus').get_by_text('Part 1 estimated part length:',exact=False).wait_for()
  checks.append({'actualClientClockShiftMs':shift,'actualPINInspectZIP':True,'clientResetCannotChangeServerReceipt':True});page.close()
 context.close();browser.close()
assert not errors,errors
(art/'result.json').write_text(json.dumps({'checks':checks,'requests':requests,'pageErrors':errors,'offOriginBlocked':denied},indent=2))
print(json.dumps({'checks':len(checks),'requests':len(requests),'pageErrors':0,'loopbackOnly':True}))
