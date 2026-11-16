"""Original actual Date/expiry held across measured local response delay."""
import argparse,hashlib,json,time
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright
p=argparse.ArgumentParser();p.add_argument('--base-url',required=True);p.add_argument('--chromium',required=True);p.add_argument('--artifacts',required=True);a=p.parse_args();origin=urlsplit(a.base_url);assert origin.hostname in ('127.0.0.1','localhost')
art=Path(a.artifacts);art.mkdir(parents=True,exist_ok=True);checks=[];failures=[]
with sync_playwright() as d:
 browser=d.chromium.launch(executable_path=a.chromium,headless=True,args=['--no-sandbox']);context=browser.new_context(accept_downloads=True)
 def only_local(route):
  u=urlsplit(route.request.url)
  if (u.scheme,u.netloc)==(origin.scheme,origin.netloc):route.continue_()
  else:route.abort()
 context.route('**/*',only_local)
 def settled():
  deadline=time.monotonic()+8
  while not delivered['done']:
   assert time.monotonic()<deadline
   page.wait_for_timeout(10)
  page.wait_for_timeout(300)
 context.request.get(a.base_url+'/fixture/sign-in?role=account');info=context.request.get(a.base_url+'/fixture/info').json()
 # Prime a genuine FD observation. Only the separate fixture port exposes a
 # two-second TTL; actual metadata/Date/expiry then stay unchanged while held.
 profile=info['open'];job='job-'+profile
 r=context.request.post(a.base_url+'/account/api/live/recordings/'+job+'/parts/1/timing/refresh',headers={'Origin':a.base_url,'Sec-Fetch-Site':'same-origin'},form={'profileId':profile,'profileUnlock':''});assert r.status==200 and r.json()['state']=='container_estimate'
 context.request.get(a.base_url+'/fixture/timing-mode?value=short-timing')
 page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(profile);read=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=read]');read.wait_for()
 delivered={'done':False}
 def delayed(route):
  response=route.fetch(max_redirects=0);assert response.status==200
  raw=response.body();data=response.json();assert data['state']=='container_estimate'
  headers={k:v for k,v in response.headers.items() if k.lower() not in ('content-length','content-encoding','transfer-encoding')}
  date=headers['date'];expiry=data['expiresAt'];start=time.monotonic();time.sleep(3)
  route.fulfill(status=200,body=raw,headers=headers)
  delivered['done']=True
  checks.append({'kind':'timing_short_ttl_transport_port','originalDate':date,'originalExpiry':expiry,'sameBytesSha256':hashlib.sha256(raw).hexdigest(),'heldSeconds':round(time.monotonic()-start,3),'actualPriorFD':True})
 page.route(a.base_url+'/account/api/live/recordings/'+job+'/parts/1/timing/read',delayed);read.click();settled()
 text=page.locator('#recordingExportStatus').inner_text()
 if not text.startswith('Estimate expired.'):failures.append({'case':'timing','expiredExpected':True,'numericShown':'estimated part length:' in text})
 page.close()
 # The PIN verifier really issues a two-second token. Its actual response body
 # and HTTP-Date are retained; no fixture rewrite invents a new expiry/token.
 context.request.get(a.base_url+'/fixture/timing-mode?value=short-unlock');profile=info['locked'];job='job-'+profile;page=context.new_page();page.goto(a.base_url+'/account/recordings');page.locator('#recordingProfile').select_option(profile)
 posts=[]
 page.on('request',lambda request:posts.append(True) if '/timing/refresh' in request.url else None)
 button=page.locator('[data-recording-timing="'+job+'"][data-timing-operation=inspect]');button.wait_for();button.click();dialog=page.locator('#recordingExportPIN');dialog.wait_for(state='visible');before=len(posts)
 delivered={'done':False}
 def delayed_pin(route):
  response=route.fetch(max_redirects=0);assert response.status==200 and response.json()['valid'] is True
  raw=response.body();expiry=response.json()['expiresAt'];headers={k:v for k,v in response.headers.items() if k.lower() not in ('content-length','content-encoding','transfer-encoding')};date=headers['date'];start=time.monotonic();time.sleep(3)
  route.fulfill(status=200,body=raw,headers=headers)
  delivered['done']=True
  # Opaque token deliberately never saved in logs/artifacts.
  checks.append({'kind':'actual_short_pin','originalDate':date,'originalExpiry':expiry,'bodyUnchanged':True,'heldSeconds':round(time.monotonic()-start,3)})
 page.route(a.base_url+'/account/api/users/'+profile+'/pin/verify',delayed_pin);dialog.locator('[data-export-pin]').fill('1234');dialog.get_by_role('button',name='Unlock and inspect',exact=True).click();settled()
 if len(posts)!=before:failures.append({'case':'PIN','expiredUnlockAdoptedAndReposted':True})
 if 'PIN check unavailable' not in dialog.locator('[data-export-pin-status]').inner_text():failures.append({'case':'PIN','expiredResponseNotRejectedLocally':True})
 page.close();context.close();browser.close()
(art/'result.json').write_text(json.dumps({'checks':checks,'failures':failures},indent=2))
assert not failures,'Original response delay was not bounded: '+json.dumps(failures)
print(json.dumps({'checks':len(checks),'originalDateExpiryUnchanged':True,'failures':0}))
