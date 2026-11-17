const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');
const intent = digit => 'pli_' + digit.repeat(64);

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
    const fetch = (options = {}, current = () => true) => window.parentListRemovalFetch('https://server.test/account/api/users/profile-a/kids/lists?url=https%3A%2F%2Fmdblist.com%2Fa%3Fq%3D%252F%2B%26x%3D%27%E9%9B%AA', {method:'DELETE', ...options}, current);
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


test('DELETE keeps the exact original query and sends no body or replacement URL', async () => {
    const p = app(request => request.intent ? p.response(200, {id:'profile-a',kidsAllowedLists:[]}) : p.challenge(intent('a')));
    const pending = p.fetch(); await p.respond('  synthetic 雪 password  ');
    assert.equal((await pending).status, 200); assert.equal(p.calls.length, 2);
    assert.equal(p.calls[1].url, p.calls[0].url); assert.equal(p.calls[1].method, 'DELETE');
    assert.equal(p.calls[0].body, ''); assert.equal(p.calls[1].body, '');
    assert.equal(p.calls[1].intent, intent('a'));
    assert.equal(Buffer.from(p.calls[1].password.slice(7),'base64').toString('utf8'), '  synthetic 雪 password  ');
});

test('mode and list-removal intents cannot authorize each other or unbound DELETE', async () => {
    const p = app(() => p.challenge(intent('a')));
    assert.equal((await p.window.parentModeFetch('https://server.test/api/users/profile-a/kids/mode', {method:'PUT',body:'{"mode":"rating"}'}, () => true)).status, 403);
    assert.equal((await p.window.fetch('https://server.test/api/users/profile-a/kids/lists?url=stored', {method:'DELETE'})).status, 403);
    assert.equal(p.dialogs.length, 0);
    const other = app(() => other.challenge('pmi_'+'a'.repeat(64)));
    assert.equal((await other.fetch()).status,403); assert.equal(other.dialogs.length,0);
});

test('present malformed tokens and non-bodyless or wrong-method actions never prompt', async () => {
    for (const token of [null,'','pli_'+'A'.repeat(64),intent('a')+' ', 'foreign']) {
        const p = app(() => p.challenge(token));
        assert.equal((await p.fetch()).status,403); assert.equal(p.dialogs.length,0);
    }
    for (const options of [{method:'POST',body:'{}'}, {body:'unexpected'}]) {
        const p = app(() => p.challenge(intent('a')));
        assert.equal((await p.fetch(options)).status,403); assert.equal(p.dialogs.length,0);
    }
});

test('Cancel Escape signal page and original-row retirement cause no DELETE retry', async () => {
    for (const kind of ['cancel','escape','signal','page','row']) {
        const p = app(() => p.challenge(intent('a'))), controller = new AbortController();
        let current = true;
        const pending = p.fetch({signal:controller.signal},()=>current);
        const rejected = ['signal','page','row'].includes(kind) ? assert.rejects(pending,e=>e.name==='AbortError') : null;
        await p.dialog();
        if (kind==='signal') controller.abort();
        else if (kind==='page') p.pagehide();
        else if (kind==='row') {current=false;await p.respond('synthetic stale password');}
        else await p.respond(kind==='escape'?'escape':null);
        if (rejected) await rejected; else assert.equal((await pending).status,403);
        assert.equal(p.calls.length,1); assert.equal(p.active().length,0);
    }
});

test('two delete intents never share a password', async () => {
    let n=0;
    const p=app(request=>request.intent?p.response(200,{}):p.challenge(intent(++n===1?'a':'b')));
    const first=p.fetch();await p.dialog();const second=p.fetch();
    for(let i=0;i<100&&p.dialogs.length<2;i++) await new Promise(resolve=>setTimeout(resolve,1));
    assert.equal((await first).status,403);await p.respond('synthetic second password');
    assert.equal((await second).status,200);assert.equal(p.calls.filter(r=>r.password!==null).length,1);
    assert.equal(p.calls[2].intent,intent('b'));
});

test('removal passwords cannot be shared with mode or legacy mutations',async()=>{
    for(const other of ['mode','legacy']){
        const p=app(request=>{
            if(request.password) return p.response(200,{});
            if(request.url.includes('/kids/lists?')) return p.challenge(intent('a'));
            if(other==='mode') return p.challenge('pmi_'+'b'.repeat(64));
            return p.response(403,{code:'parent_authorization_required'});
        });
        const first=other==='mode'
            ?p.window.parentModeFetch('https://server.test/api/users/profile-a/kids/mode',{method:'PUT',body:'{"mode":"rating"}'},()=>true)
            :p.window.fetch('https://server.test/api/legacy-mutation',{method:'PUT',body:'{}'});
        await p.dialog();const removal=p.fetch();
        for(let i=0;i<100&&p.dialogs.length<2;i++) await new Promise(resolve=>setTimeout(resolve,1));
        assert.equal((await first).status,403);await p.respond('synthetic removal password');
        assert.equal((await removal).status,200);
        const confirmed=p.calls.filter(request=>request.password!==null);
        assert.equal(confirmed.length,1);assert.equal(confirmed[0].intent,intent('a'));
    }
});

test('expired wrong-password changed and unknown outcomes never replace the intent',async()=>{
    for(const [status,code] of [[409,'parent_confirmation_changed'],[503,'parent_confirmation_unavailable'],[503,'parent_confirmation_outcome_unknown'],[403,'parent_authorization_required'],[429,'rate_limited']]){
        const p=app(request=>request.intent?p.response(status,{code,parentIntent:intent('b')}):p.challenge(intent('a')));
        const pending=p.fetch();await p.respond('synthetic password');assert.equal((await pending).status,status);
        assert.equal(p.calls.length,2);assert.equal(p.dialogs.length,1);
    }
});

test('late first reply or late committed reply cannot publish as current',async()=>{
    for(const stage of ['first','retry']){
        let release;
        const p=app(request=>stage==='first'||request.intent?new Promise(resolve=>{release=resolve;}):p.challenge(intent('a'))),controller=new AbortController();
        const pending=p.fetch({signal:controller.signal}),rejected=assert.rejects(pending,e=>e.name==='AbortError');
        if(stage==='retry') await p.respond('synthetic password');
        for(let i=0;i<100&&!release;i++) await new Promise(resolve=>setTimeout(resolve,1));
        controller.abort();release(stage==='first'?p.challenge(intent('a')):p.response(200,{id:'profile-a'}));await rejected;
        assert.equal(p.calls.length,stage==='first'?1:2);
    }
});

test('local maximum wait never retries or renews server authority',async()=>{
    const p=app(()=>p.challenge(intent('a'))),pending=p.fetch();await p.dialog();[...p.timers][0]();
    assert.equal((await pending).status,403);assert.equal(p.calls.length,1);assert.equal(p.active().length,0);
});
