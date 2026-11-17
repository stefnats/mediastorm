const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');
const intent = digit => 'pmi_' + digit.repeat(64);

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
    const fetch = (options = {}, current = () => true) => window.parentModeFetch('https://server.test/account/api/users/profile-a/kids/mode', {method:'PUT', body:'{"mode":"rating"}', headers:{'Content-Type':'application/json'}, ...options}, current);
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

test('PG retry retains its exact original body, method, URL, intent and UTF-8 password', async () => {
    const p = app(request => request.intent ? p.response(200, {id:'profile-a',kidsMode:'rating'}) : p.challenge(intent('a')));
    const password = '  synthetic パスワード  ';
    const pending = p.fetch();
    await p.respond(password);
    assert.equal((await pending).status, 200);
    assert.equal(p.calls.length, 2);
    assert.equal(p.calls[0].intent, null); assert.equal(p.calls[0].password, null);
    assert.equal(p.calls[1].method, p.calls[0].method); assert.equal(p.calls[1].url, p.calls[0].url); assert.equal(p.calls[1].body, p.calls[0].body);
    assert.equal(p.calls[1].intent, intent('a'));
    assert.equal(Buffer.from(p.calls[1].password.slice(7), 'base64').toString('utf8'), password);
    assert.equal(p.timers.size, 0);
});

test('different PG intents cannot share the first dialog password', async () => {
    let first = 0;
    const p = app(request => request.intent ? p.response(200, {ok:true}) : p.challenge(intent(++first === 1 ? 'a' : 'b')));
    const one = p.fetch(); await p.dialog();
    const two = p.fetch();
    for (let i = 0; i < 100 && p.dialogs.length < 2; i++) await new Promise(resolve => setTimeout(resolve, 1));
    assert.equal(p.dialogs.length, 2); assert.equal(p.active().length, 1);
    assert.equal((await one).status, 403);
    await p.respond('synthetic second password');
    assert.equal((await two).status, 200);
    assert.equal(p.calls.filter(request => request.password !== null).length, 1);
    assert.equal(p.calls[2].intent, intent('b'));
});

test('a PG prompt does not consume the legacy shared password promise', async () => {
    const p = app(request => request.password ? p.response(200, {ok:true}) : request.url.endsWith('/kids/mode') ? p.challenge(intent('a')) : p.response(403, {code:'parent_authorization_required',error:'Legacy confirmation.'}));
    const legacy = p.window.fetch('https://server.test/account/api/legacy-mutation', {method:'PUT',body:'{}'});
    await p.dialog();
    const bound = p.fetch();
    for (let i = 0; i < 100 && p.dialogs.length < 2; i++) await new Promise(resolve => setTimeout(resolve, 1));
    assert.equal((await legacy).status, 403);
    await p.respond('synthetic bound password');
    assert.equal((await bound).status, 200);
    assert.equal(p.calls.filter(request => request.password !== null).length, 1);
    assert.ok(p.calls.filter(request => request.password !== null).every(request => request.intent === intent('a')));
});

test('Cancel and Escape consume no retry request', async () => {
    for (const choice of [null, 'escape']) {
        const p = app(() => p.challenge(intent('a'))), pending = p.fetch();
        await p.respond(choice);
        assert.equal((await pending).status, 403); assert.equal(p.calls.length, 1); assert.equal(p.timers.size, 0);
    }
});

test('original signal and actual form predicate block stale confirmation before retry', async () => {
    for (const mode of ['signal', 'predicate', 'pagehide']) {
        const p = app(() => p.challenge(intent('a'))), controller = new AbortController();
        let current = true;
        const pending = p.fetch({signal:controller.signal}, () => current);
        const rejected = assert.rejects(pending, error => error.name === 'AbortError');
        await p.dialog();
        if (mode === 'signal') controller.abort();
        else if (mode === 'pagehide') p.pagehide();
        else { current = false; await p.respond('synthetic stale password'); }
        await rejected;
        assert.equal(p.calls.length, 1); assert.equal(p.active().length, 0);
    }
});

test('a late first challenge cannot open a dialog after original cancellation', async () => {
    let release;
    const p = app(() => new Promise(resolve => { release = resolve; })), controller = new AbortController();
    const pending = p.fetch({signal:controller.signal});
    const rejected = assert.rejects(pending, error => error.name === 'AbortError');
    for (let i = 0; i < 100 && !release; i++) await new Promise(resolve => setTimeout(resolve, 1));
    controller.abort(); release(p.challenge(intent('a'))); await rejected;
    assert.equal(p.dialogs.length, 0); assert.equal(p.calls.length, 1);
});

test('late successful retry is not returned as current after cancellation', async () => {
    let release;
    const p = app(request => request.intent ? new Promise(resolve => { release = resolve; }) : p.challenge(intent('a'))), controller = new AbortController();
    const pending = p.fetch({signal:controller.signal});
    const rejected = assert.rejects(pending, error => error.name === 'AbortError');
    await p.respond('synthetic password');
    for (let i = 0; i < 100 && !release; i++) await new Promise(resolve => setTimeout(resolve, 1));
    controller.abort(); release(p.response(200, {id:'profile-a',kidsMode:'rating'})); await rejected;
    assert.equal(p.calls.length, 2); assert.equal(p.dialogs.length, 1);
});

test('malformed present intent cannot downgrade to legacy password prompting', async () => {
    for (const token of [null, '', 'pmi_'+'A'.repeat(64), intent('a')+' ', 'foreign']) {
        const p = app(() => p.challenge(token));
        assert.equal((await p.fetch()).status, 403); assert.equal(p.dialogs.length, 0); assert.equal(p.calls.length, 1);
    }
});

test('expired consumed unavailable and unknown retries never start another challenge', async () => {
    for (const [status, code] of [[409,'parent_confirmation_changed'],[503,'parent_confirmation_unavailable'],[503,'parent_confirmation_outcome_unknown'],[403,'parent_authorization_required'],[429,'rate_limited']]) {
        const p = app(request => request.intent ? p.response(status, {code,error:'Fixed failure.',parentIntent:intent('b')}) : p.challenge(intent('a')));
        const pending = p.fetch(); await p.respond('synthetic password');
        assert.equal((await pending).status, status); assert.equal(p.calls.length, 2); assert.equal(p.dialogs.length, 1);
    }
});

test('local maximum dialog wait closes without adopting a new server deadline', async () => {
    const p = app(() => p.challenge(intent('a'))), pending = p.fetch();
    await p.dialog(); assert.equal(p.timers.size, 1);
    [...p.timers][0]();
    assert.equal((await pending).status, 403); assert.equal(p.active().length, 0); assert.equal(p.calls.length, 1);
});
