"""Actual cookie/PIN/template/hidden POST/ZIP path with synthetic files."""
import argparse,json,zipfile
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from playwright.sync_api import sync_playwright
p=argparse.ArgumentParser();p.add_argument('--base-url',required=True);p.add_argument('--chromium',required=True);p.add_argument('--artifacts',required=True);a=p.parse_args()
assert urlsplit(a.base_url).hostname in ('127.0.0.1','localhost')
artifacts=Path(a.artifacts);artifacts.mkdir(parents=True,exist_ok=True)
errors=[];requests=[];checks=[]
with sync_playwright() as driver:
 browser=driver.chromium.launch(executable_path=a.chromium,headless=True,args=['--no-sandbox'])
 context=browser.new_context(accept_downloads=True)
 info=context.request.get(a.base_url+'/fixture/info').json();assert info['fixture']=='synthetic-recording-export-web'
 for role in ['admin','account']:
  assert context.request.get(a.base_url+'/fixture/sign-in?role='+role).status==204
  for width in [320,390,1280]:
   page=context.new_page();page.set_viewport_size({'width':width,'height':1000})
   page.on('pageerror',lambda e:errors.append(str(e)))
   def observe(req):
    if '/export' not in urlsplit(req.url).path or req.method!='POST':return
    assert not urlsplit(req.url).query,'Export credentials entered URL'
    fields=parse_qs(req.post_data or '',keep_blank_values=True)
    assert set(fields)<=set(['profileId','profileUnlock']) and fields['profileId'][0] in [info['locked'],info['open']]
    assert all(len(v)==1 for v in fields.values())
    requests.append({'method':req.method,'noQuery':True,'opaqueUnlockPresent':bool(fields.get('profileUnlock',[''])[0]),'profileMatches':True})
   page.on('request',observe)
   page.goto(a.base_url+'/'+role+'/recordings')
   page.locator('#recordingProfile').select_option(info['locked'])
   button=page.locator('[data-recording-export="job-'+info['locked']+'"]');button.wait_for()
   assert '2 original parts' in page.locator('#recordingsList').inner_text()
   assert 'Incomplete capture' in page.locator('#recordingsList').inner_text()
   assert 'Play first available part' in page.locator('#recordingsList').inner_text()
   before=context.request.get(a.base_url+'/fixture/info').json()['opens']
   button.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible')
   assert context.request.get(a.base_url+'/fixture/info').json()['opens']==before
   if role=='admin' and width==320:
    dialog.locator('[data-export-pin]').fill('wrong')
    dialog.get_by_role('button',name='Unlock and download',exact=True).click()
    dialog.get_by_text('Incorrect PIN. Try again.',exact=True).wait_for()
    assert context.request.get(a.base_url+'/fixture/info').json()['opens']==before
   dialog.locator('[data-export-pin]').fill('1234')
   with page.expect_download() as download_event:
    dialog.get_by_role('button',name='Unlock and download',exact=True).click()
   download=download_event.value
   assert download.failure() is None
   path=Path(download.path())
   with zipfile.ZipFile(path) as archive:
    assert set(archive.namelist())=={'part-000001.ts','part-000003.ts','manifest.json'}
    assert all(entry.compress_type==zipfile.ZIP_STORED for entry in archive.infolist())
    expected=b'SYNTHETIC_WEB_EXPORT_MEDIA\n'*64
    assert archive.read('part-000001.ts')==expected and archive.read('part-000003.ts')==expected
    manifest=json.loads(archive.read('manifest.json'));assert manifest['incomplete'] is True and manifest['status']=='failed'
    assert b'PRIVATE_' not in path.read_bytes()
   assert context.request.get(a.base_url+'/fixture/info').json()['opens']==before+1
   assert 'Download requested' in page.locator('#recordingExportStatus').inner_text()
   assert 'completion' in page.locator('#recordingExportStatus').inner_text()
   assert page.locator('#recordingExportPINInput').input_value()==''
   assert not page.locator('form input[name=profileUnlock]').count()
   assert not page.evaluate('Object.keys(localStorage).some(k=>/unlock|pin/i.test(k))')
   assert 'profileUnlock' not in page.url and '1234' not in page.url
   if role=='account':page.screenshot(path=str(artifacts/('recording-export-'+str(width)+'.png')),full_page=False)
   checks.append({'role':role,'width':width,'actualZIP':True,'incompletePreserved':True,'originalBytes':True,'noURLCredential':True})
   page.close()

 # A delayed control reply must not replace a newer explicit action B.
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['locked'])
 first=page.locator('[data-recording-export="job-'+info['locked']+'"]');second=page.locator('[data-recording-export="second-'+info['locked']+'"]');first.wait_for();second.wait_for()
 assert context.request.get(a.base_url+'/fixture/hold?kind=denial').status==204
 first.click()
 def state():return context.request.get(a.base_url+'/fixture/info').json()
 import time
 deadline=time.monotonic()+5
 while not state()['waitingDenied']:
  assert time.monotonic()<deadline;page.wait_for_timeout(10)
 second.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible')
 assert context.request.get(a.base_url+'/fixture/release').status==204
 page.wait_for_timeout(100)
 dialog.locator('[data-export-pin]').fill('1234')
 with page.expect_download() as event:dialog.get_by_role('button',name='Unlock and download',exact=True).click()
 download=event.value
 assert download.failure() is None and ('second-'+info['locked']) in download.suggested_filename,'Old action reply replaced latest export intent'
 checks.append({'lateOldActionIgnored':True});page.close()

 # Profile change cancels a pending real PIN verification result without a new POST.
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['locked']);first=page.locator('[data-recording-export="job-'+info['locked']+'"]');first.wait_for();first.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible')
 before=state()['opens'];assert context.request.get(a.base_url+'/fixture/hold?kind=pin').status==204
 dialog.locator('[data-export-pin]').fill('1234');dialog.get_by_role('button',name='Unlock and download',exact=True).click()
 deadline=time.monotonic()+5
 while not state()['waitingPIN']:
  assert time.monotonic()<deadline;page.wait_for_timeout(10)
 page.locator('#recordingProfile').select_option(info['open']);assert context.request.get(a.base_url+'/fixture/release').status==204
 page.locator('[data-recording-export="job-'+info['open']+'"]') .wait_for()
 assert not dialog.is_visible() and state()['opens']==before
 checks.append({'latePINProfileChangeNoDownload':True})

 # Browser cancellation must finish the actual server transfer and free both slots.
 assert context.request.get(a.base_url+'/fixture/large').status==204
 before=state()['finished']
 with page.expect_download() as event:page.locator('[data-recording-export="job-'+info['open']+'"]') .click()
 download=event.value;download.cancel()
 assert download.failure() is not None
 deadline=time.monotonic()+5
 while state()['finished']<=before:
  assert time.monotonic()<deadline;page.wait_for_timeout(10)
 assert context.request.get(a.base_url+'/fixture/slots').json()['free'] is True
 checks.append({'browserCancelServerJoinedAndSlotsFree':True});page.close()

 # Actual browser transfer is failed, not successfully finalized, on PIN replacement.
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(info['locked']);button=page.locator('[data-recording-export="job-'+info['locked']+'"]');button.wait_for()
 assert context.request.get(a.base_url+'/fixture/retire').status==204
 button.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible');dialog.locator('[data-export-pin]').fill('1234')
 with page.expect_download() as event:dialog.get_by_role('button',name='Unlock and download',exact=True).click()
 failed=event.value;assert failed.failure() is not None,'Retired PIN transfer completed in browser'
 assert 'Download completed' not in page.locator('#recordingExportStatus').inner_text()
 assert context.request.get(a.base_url+'/fixture/slots').json()['free'] is True
 checks.append({'PINRetiredActualTransferFailed':True});page.close()
 assert not errors,errors
 context.close();browser.close()
(artifacts/'results.json').write_text(json.dumps({'checks':checks,'requests':requests,'pageErrors':errors,'scope':'Actual embedded template/static JS, real cookie session/PIN handlers and real ZIP/FD export; synthetic files, no password login/providers/media-codec claim.'},indent=2)+'\n')
