const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');
const intent = digit => 'prp_' + digit.repeat(64);

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
    const fetch = (options = {}, current = () => true) => window.parentRatingFetch('https://server.test/account/api/users/profile-a/kids/ratings', {method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({movieRating:'',tvRating:''}), ...options}, current);
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




test('one pair request freezes both original strings and retries identical bytes once',async()=>{
    const raw=' {"movieRating":null,"movieRating":" saved 雪 ","tvRating":" TV-Y "} trailing';
    const p=app(request=>request.intent?p.response(200,{}):p.challenge(intent('a')));
    const pending=p.fetch({body:raw});await p.respond('  synthetic 雪 password  ');
    assert.equal((await pending).status,200);assert.equal(p.calls.length,2);
    assert.equal(p.calls[0].body,raw);assert.equal(p.calls[1].body,raw);
    assert.equal(p.calls[1].method,'PUT');assert.equal(p.calls[1].url,p.calls[0].url);assert.ok(p.calls[1].url.endsWith('/kids/ratings'));
    assert.equal(p.calls[1].intent,intent('a'));assert.equal(Buffer.from(p.calls[1].password.slice(7),'base64').toString('utf8'),'  synthetic 雪 password  ');
});

test('empty pair explicitly transmits both fields without a generic rating field',async()=>{
    const p=app(request=>request.intent?p.response(200,{}):p.challenge(intent('a'))),pending=p.fetch();await p.respond('synthetic password');await pending;
    assert.deepEqual(JSON.parse(p.calls[0].body),{movieRating:'',tvRating:''});assert.equal(p.calls[1].body,p.calls[0].body);
});

test('pair rejects scalar and other-action prefixes and never prompts through unbound fetch',async()=>{
    for(const prefix of ['prg_','prm_','prt_','pmi_','pli_','pla_','plw_']){
        const p=app(()=>p.challenge(prefix+'a'.repeat(64)));assert.equal((await p.fetch()).status,403);assert.equal(p.dialogs.length,0);
    }
    const p=app(()=>p.challenge(intent('a')));
    assert.equal((await p.window.fetch('https://server.test/api/users/profile-a/kids/ratings',{method:'PUT',body:'{}'})).status,403);assert.equal(p.dialogs.length,0);
});

test('Cancel Escape and local timeout send no pair or scalar retry',async()=>{
    for(const choice of [null,'escape','timeout']){
        const p=app(()=>p.challenge(intent('a'))),pending=p.fetch();await p.dialog();
        if(choice==='timeout')[...p.timers][0]();else await p.respond(choice);
        assert.equal((await pending).status,403);assert.equal(p.calls.length,1);
    }
});

test('original select form profile mode and page retirement prevents a pair retry',async()=>{
    for(const kind of ['signal','movie','tv','form','profile','mode','page']){
        const p=app(()=>p.challenge(intent('a'))),controller=new AbortController();let current=true;
        const pending=p.fetch({signal:controller.signal},()=>current),rejected=assert.rejects(pending,e=>e.name==='AbortError');await p.dialog();
        if(kind==='signal')controller.abort();else if(kind==='page')p.pagehide();else{current=false;await p.respond('synthetic stale password');}
        await rejected;assert.equal(p.calls.length,1);assert.equal(p.active().length,0);
    }
});

test('different pair actions never share the first password',async()=>{
    let n=0;const p=app(request=>request.intent?p.response(200,{}):p.challenge(intent(++n===1?'a':'b')));
    const one=p.fetch();await p.dialog();const two=p.fetch();
    for(let i=0;i<100&&p.dialogs.length<2;i++)await new Promise(resolve=>setTimeout(resolve,1));
    assert.equal((await one).status,403);await p.respond('synthetic second password');assert.equal((await two).status,200);
    const confirmed=p.calls.filter(request=>request.password!==null);assert.equal(confirmed.length,1);assert.equal(confirmed[0].intent,intent('b'));
});

test('pair does not consume a legacy mutation password promise',async()=>{
    const p=app(request=>request.password?p.response(200,{}):request.url.endsWith('/kids/ratings')?p.challenge(intent('a')):p.response(403,{code:'parent_authorization_required'}));
    const legacy=p.window.fetch('https://server.test/api/legacy-mutation',{method:'PUT',body:'{}'});await p.dialog();const pair=p.fetch();
    for(let i=0;i<100&&p.dialogs.length<2;i++)await new Promise(resolve=>setTimeout(resolve,1));
    assert.equal((await legacy).status,403);await p.respond('synthetic pair password');assert.equal((await pair).status,200);
    const confirmed=p.calls.filter(request=>request.password!==null);assert.equal(confirmed.length,1);assert.equal(confirmed[0].intent,intent('a'));
});

test('failed or unknown pair responses never downgrade to old single-field routes',async()=>{
    for(const [status,code] of [[409,'parent_confirmation_changed'],[503,'parent_confirmation_unavailable'],[503,'parent_confirmation_outcome_unknown'],[403,'parent_authorization_required'],[429,'rate_limited'],[404,'not_found']]){
        const p=app(request=>request.intent?p.response(status,{code,parentIntent:intent('b')}):p.challenge(intent('a'))),pending=p.fetch();
        await p.respond('synthetic password');assert.equal((await pending).status,status);assert.equal(p.calls.length,2);assert.equal(p.dialogs.length,1);
        assert.ok(p.calls.every(request=>request.url.endsWith('/kids/ratings')));
    }
});

test('late first or committed pair reply is ignored after original action ends',async()=>{
    for(const stage of ['first','retry']){
        let release;const p=app(request=>stage==='first'||request.intent?new Promise(resolve=>{release=resolve;}):p.challenge(intent('a'))),controller=new AbortController();
        const pending=p.fetch({signal:controller.signal}),rejected=assert.rejects(pending,e=>e.name==='AbortError');
        if(stage==='retry')await p.respond('synthetic password');for(let i=0;i<100&&!release;i++)await new Promise(resolve=>setTimeout(resolve,1));
        controller.abort();release(stage==='first'?p.challenge(intent('a')):p.response(200,{id:'profile-a'}));await rejected;
        assert.equal(p.calls.length,stage==='first'?1:2);
    }
});
