const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');

function app(reply) {
    const calls = [], dialogs = [], timers = new Set(), listeners = {};
    class Element {
        constructor(tag) { this.tagName = tag; this.children = []; this.events = {}; this.style = {}; this.value = ''; }
        append(...children) { this.children.push(...children); }
        appendChild(child) { this.append(child); }
        setAttribute(name, value) { this[name] = value; }
        addEventListener(name, fn) { this.events[name] = fn; }
        showModal() { dialogs.push(this); }
        focus() {}
        remove() { this.removed = true; }
    }
    const window = {
        location: {origin: 'https://server.test'},
        addEventListener(name, callback) { listeners[name] = callback; },
        fetch: async request => {
            const value = {method: request.method, url: request.url, body: await request.clone().text(), password: request.headers.get('X-Account-Password'), intent: request.headers.get('X-Parent-Intent'), signal: request.signal};
            calls.push(value);
            return reply(value, calls.length);
        }
    };
    const context = vm.createContext({window, document: {body: new Element('body'), createElement: tag => new Element(tag)}, Request, Response, Headers, URL, Promise, TextEncoder, btoa,
        setTimeout(callback) { timers.add(callback); return callback; }, clearTimeout(callback) { timers.delete(callback); }});
    vm.runInContext(source, context);
    const response = (status, body) => new Response(JSON.stringify(body), {status, headers: {'Content-Type':'application/json'}});
    const challenge = token => response(403, {code:'parent_authorization_required', error:'Confirm the account password.', parentIntent:token});
    const fetch = (options = {}, current = () => true) => window.parentProfileFlagFetch('https://server.test/account/api/profiles/kids?profileId=profile-a', {method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({isKidsProfile:false}), ...options}, current);
    const active = () => dialogs.filter(dialog => !dialog.removed);
    async function dialog() {
        for (let i = 0; i < 100; i++) {
            if (active().length) return active()[0];
            await new Promise(resolve => setTimeout(resolve, 1));
        }
        throw Error('Bound confirmation did not open');
    }
    async function respond(value) {
        const d = await dialog(), form = d.children[0], input = form.children.find(element => element.tagName === 'label').children[0];
        if (value === 'escape') d.events.cancel({preventDefault() {}});
        else if (value === null) form.children.find(element => element.textContent === 'Cancel').events.click();
        else { input.value = value; form.events.submit({preventDefault() {}}); }
        assert.equal(input.value, ''); assert.equal(d.removed, true);
    }
    return {calls, dialogs, active, dialog, respond, fetch, window, timers, response, challenge, pagehide: () => listeners.pagehide()};
}





const token = prefix => prefix + 'a'.repeat(64);
async function until(predicate) {
    for (let i=0;i<200&&!predicate();i++) await new Promise(resolve=>setTimeout(resolve,1));
    assert.ok(predicate(), 'Synthetic async barrier did not arrive');
}
test('flag retries the exact original body path query and signal once with its own password', async () => {
    const p = app(request => request.intent ? p.response(200,{}) : p.challenge(token('pkf_')));
    const raw = ' {"isKidsProfile":true} ';
    const pending = p.fetch({body:raw}); await p.respond('  synthetic 雪 password  '); await pending;
    assert.equal(p.calls.length,2); assert.equal(p.calls[0].url,p.calls[1].url);
    assert.equal(p.calls[0].body,raw); assert.equal(p.calls[1].body,raw);
    assert.equal(p.calls[1].intent,token('pkf_'));
    assert.equal(Buffer.from(p.calls[1].password.slice(7),'base64').toString('utf8'),'  synthetic 雪 password  ');
    assert.equal(p.calls[1].signal.aborted,false);
});
test('mode list and ratings challenges cannot enter the flag password dialog', async () => {
    for (const prefix of ['pmi_','pli_','pla_','plw_','prp_','prg_','prm_','prt_']) {
        const p=app(()=>p.challenge(token(prefix))); assert.equal((await p.fetch()).status,403); assert.equal(p.dialogs.length,0);
    }
    const p=app(()=>p.challenge(token('pkf_')));
    await assert.rejects(p.fetch({},null)); assert.equal(p.calls.length,0);
    assert.equal((await p.window.fetch('https://server.test/account/api/profiles/kids?profileId=profile-a',{method:'PUT',body:'{}'})).status,403);
    assert.equal(p.dialogs.length,0);
});
test('each new flag intent needs a separate confirmation instead of joining another password promise', async () => {
    let n=0; const p=app(request=>request.intent?p.response(200,{}):p.challenge((++n===1?'pkf_'+'a'.repeat(64):'pkf_'+'b'.repeat(64))));
    const one=p.fetch();await p.dialog();const two=p.fetch();
    await until(()=>p.dialogs.length===2);
    assert.equal((await one).status,403); await p.respond('second synthetic password');await two;
    assert.equal(p.calls.filter(call=>call.password).length,1);
    assert.equal(p.calls.at(-1).intent,'pkf_'+'b'.repeat(64));
});
test('Cancel Escape and local timeout never issue a flag retry', async () => {
    for(const choice of [null,'escape','timeout']){
        const p=app(()=>p.challenge(token('pkf_'))),pending=p.fetch(); await p.dialog();
        if(choice==='timeout')[...p.timers][0]();else await p.respond(choice);
        assert.equal((await pending).status,403);assert.equal(p.calls.length,1);
    }
});
test('form profile input scope and page retirement stop before password retry', async () => {
    for(const reason of ['form','profile','input','scope','page','signal']) {
        const p=app(()=>p.challenge(token('pkf_'))),controller=new AbortController();let current=true;
        const pending=p.fetch({signal:controller.signal},()=>current),rejected=assert.rejects(pending,e=>e.name==='AbortError');await p.dialog();
        if(reason==='page')p.pagehide();else if(reason==='signal')controller.abort();else{current=false;await p.respond('stale synthetic password');}
        await rejected;assert.equal(p.calls.length,1);assert.equal(p.active().length,0);
    }
});
test('changed expired denied and unknown outcomes never obtain a new challenge or downgrade', async () => {
    for(const [status,code] of [[409,'parent_confirmation_changed'],[503,'parent_confirmation_outcome_unknown'],[503,'parent_confirmation_unavailable'],[403,'parent_authorization_required'],[429,'rate_limited']]){
        const p=app(request=>request.intent?p.response(status,{code,parentIntent:token('pkf_')}):p.challenge(token('pkf_'))),pending=p.fetch();
        await p.respond('synthetic password');assert.equal((await pending).status,status);assert.equal(p.calls.length,2);assert.equal(p.dialogs.length,1);
    }
});
test('a masked response after a real retry does not turn a retired form into success', async () => {
    let release;const p=app(request=>request.intent?new Promise(resolve=>{release=resolve;}):p.challenge(token('pkf_')));
    const controller=new AbortController(),pending=p.fetch({signal:controller.signal}),rejected=assert.rejects(pending,e=>e.name==='AbortError');
    await p.respond('synthetic password');await until(()=>typeof release==='function');
    controller.abort();release(p.response(200,{}));await rejected;assert.equal(p.calls.length,2);
});
const saved = () => ({profile:{id:'profile-a',name:'saved 雪',isKidsProfile:false,kidsMode:'',kidsMaxRating:'',kidsMaxMovieRating:'',kidsMaxTVRating:'',kidsAllowedLists:[],allowLiveTakeover:false,allowRecordingInterruption:true},recovery:{usersWriteBlocked:false,settingsPendingUncertainty:true}});
test('recovery requires exactly ten safe fields and two actual boolean rights', () => {
    const p=app(()=>p.response(200,{})),validate=p.window.validateRecoveredProfile;
    assert.equal(validate(saved(),'profile-a'),true);
    for(const key of ['allowLiveTakeover','allowRecordingInterruption'])for(const kind of ['missing','null','string']){
        const value=saved();if(kind==='missing')delete value.profile[key];else value.profile[key]=kind==='null'?null:'false';
        assert.equal(validate(value,'profile-a'),false);
    }
    const value=saved();value.profile.pinHash='synthetic';assert.equal(validate(value,'profile-a'),false);
    assert.equal(validate(saved(),'other-profile'),false);
});
test('PG flag provenance projects exact true and false rights without unrelated Cookie defaults', async () => {
    for(const [live,recording] of [[false,false],[true,false],[false,true],[true,true]]){
        const value={id:'profile-a',isKidsProfile:false,allowLiveTakeover:live,allowRecordingInterruption:recording,activityPrivacy:'',name:'older',color:'#000000'};
        const p=app(request=>request.intent?p.response(200,value):p.challenge(token('pkf_'))),pending=p.fetch();
        await p.respond('synthetic password');const response=await pending;
        const profile={accountId:'actor',activityPrivacy:'shared',name:'draft name',color:'#123456'};
        Object.assign(profile,p.window.profileFlagProjection(await response.json(),'profile-a',false,response));
        assert.equal(profile.accountId,'actor');assert.equal(profile.activityPrivacy,'shared');assert.equal(profile.name,'draft name');
        assert.equal(profile.allowLiveTakeover,live);assert.equal(profile.allowRecordingInterruption,recording);
        assert.equal(profile.kidsMode,'');assert.equal(profile.kidsAllowedLists,null);
    }
});
test('legacy disable does not invent false stored rights and legacy enable always clears both', async () => {
    for(const desired of [false,true]){
        const value={id:'profile-a',isKidsProfile:desired,allowLiveTakeover:false,allowRecordingInterruption:false};
        const p=app(()=>p.response(200,value)),response=await p.fetch({body:JSON.stringify({isKidsProfile:desired})});
        const profile={allowLiveTakeover:true,allowRecordingInterruption:true};
        Object.assign(profile,p.window.profileFlagProjection(await response.json(),'profile-a',desired,response));
        assert.equal(profile.allowLiveTakeover,!desired);assert.equal(profile.allowRecordingInterruption,!desired);
        assert.equal(p.calls.length,1);assert.equal(p.dialogs.length,0);
    }
    const p=app(()=>p.response(200,{}));
    assert.throws(()=>p.window.profileFlagProjection({id:'profile-a',isKidsProfile:false,allowLiveTakeover:null,allowRecordingInterruption:false},'profile-a',false));
});
test('JSON legacy actions retain their existing concurrent shared confirmation', async () => {
    const p=app(request=>request.password?p.response(200,{}):p.response(403,{code:'parent_authorization_required'}));
    const one=p.fetch(),two=p.fetch();await until(()=>p.calls.length===2);await new Promise(resolve=>setImmediate(resolve));await p.respond('synthetic legacy password');await Promise.all([one,two]);
    assert.equal(p.dialogs.length,1);assert.equal(p.calls.filter(call=>call.password).length,2);
});

test('changed and unavailable are not proof of rollback after a possible commit',()=>{
    const p=app(()=>p.response(200,{})),refused=p.window.profileFlagDefinitelyRefused;
    for(const [status,code] of [[409,'parent_confirmation_changed'],[503,'parent_confirmation_unavailable'],[503,'parent_confirmation_outcome_unknown']])assert.equal(refused(status,{code}),false);
    assert.equal(refused(503,null),false);assert.equal(refused(200,{}),false);
    for(const [status,code] of [[403,'parent_authorization_required'],[403,'forbidden'],[403,'profile_locked'],[400,'invalid_request'],[413,'invalid_request'],[429,'rate_limited']])assert.equal(refused(status,{code}),true);
});
