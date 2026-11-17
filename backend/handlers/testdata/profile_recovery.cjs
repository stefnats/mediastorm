const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');

function app(reply) {
    const calls = [], listeners = {};
    let dialogs = 0;
    const window = {
        location: {origin: 'https://server.test'},
        addEventListener(name, fn) { listeners[name] = fn; },
        fetch: async request => {
            calls.push({request, body: await request.clone().text()});
            return reply(request, calls.length);
        }
    };
    vm.runInNewContext(source, {window, document: {createElement() { dialogs++; throw Error('Recovery opened a dialog'); }},
        Request, Response, Headers, URL, Promise, TextEncoder, btoa, setTimeout, clearTimeout});
    const fetch = (options = {}, current = () => true, url = 'https://server.test/account/api/users/profile-a/state/reload') =>
        window.profileRecoveryFetch(url, {method: 'POST', ...options}, current);
    return {window, calls, fetch, pagehide: () => listeners.pagehide(), dialogs: () => dialogs};
}
const response = (status, body = {}) => new Response(JSON.stringify(body), {status, headers: {'Content-Type': 'application/json'}});
const deferred = () => { let resolve; const promise = new Promise(fn => { resolve = fn; }); return {promise, resolve}; };

test('explicit recovery sends exactly one bodyless original request without credentials or intent', async () => {
    for (const prefix of ['/api', '/admin/api', '/account/api']) {
        const p = app(() => response(200));
        const r = await p.fetch({}, () => true, 'https://server.test' + prefix + '/users/profile-a/state/reload');
        assert.equal(r.status, 200); assert.equal(p.calls.length, 1); assert.equal(p.dialogs(), 0);
        const {request, body} = p.calls[0];
        assert.equal(body, ''); assert.equal(request.body, null); assert.equal(request.method, 'POST');
        assert.equal(request.cache, 'no-store'); assert.equal(request.redirect, 'error');
        assert.equal(request.credentials, 'same-origin'); assert.equal(request.mode, 'same-origin');
        assert.equal(request.headers.get('X-Account-Password'), null); assert.equal(request.headers.get('X-Parent-Intent'), null);
    }
});

test('every error including parent challenges is returned without a password dialog or retry', async () => {
    for (const [status, code] of [[403,'parent_authorization_required'],[403,'profile_locked'],[409,'profile_recovery_changed'],
        [503,'profile_recovery_unavailable'],[401,'unauthorized'],[404,'profile_not_found']]) {
        const p = app(() => response(status, {code, parentIntent: 'pmi_' + 'a'.repeat(64)}));
        assert.equal((await p.fetch()).status, status); assert.equal(p.calls.length, 1); assert.equal(p.dialogs(), 0);
    }
});

test('invalid method body target and confirmation headers cause zero original fetches', async () => {
    const cases = [
        [{method:'GET'}], [{body:'{}'}], [{body:''}], [{headers:{'X-Account-Password':'synthetic'}}],
        [{headers:{'X-Parent-Intent':'pli_'+'a'.repeat(64)}}],
        [{}, () => true, 'https://other.test/account/api/users/profile-a/state/reload'],
        [{}, () => true, 'https://server.test/account/api/users/profile-a/state/reload?profileId=b'],
        [{}, () => true, 'https://server.test/account/api/users/profile-a/state/reload#b'],
        [{}, () => true, 'https://server.test/account/api/users/profile-a/kids/mode'],
        [{}, null]
    ];
    for (const args of cases) {
        const p = app(() => response(200));
        await assert.rejects(p.fetch(...args)); assert.equal(p.calls.length, 0); assert.equal(p.dialogs(), 0);
    }
});

test('retired input form profile and page cause zero request before dispatch', async () => {
    const p = app(() => response(200));
    await assert.rejects(p.fetch({}, () => false), e => e.name === 'AbortError');
    const controller = new AbortController(); controller.abort();
    await assert.rejects(p.fetch({signal:controller.signal}), e => e.name === 'AbortError');
    assert.equal(p.calls.length, 0);
});

test('masked late responses cannot pass input profile page or signal retirement', async () => {
    for (const reason of ['input', 'profile', 'form', 'page', 'signal']) {
        const d = deferred(), p = app(() => d.promise), controller = new AbortController();
        let current = true;
        const pending = p.fetch({signal:controller.signal}, () => current);
        const rejected = assert.rejects(pending, e => e.name === 'AbortError');
        while (p.calls.length === 0) await new Promise(resolve => setImmediate(resolve));
        if (reason === 'page') p.pagehide(); else if (reason === 'signal') controller.abort(); else current = false;
        d.resolve(response(200)); await rejected;
        assert.equal(p.calls.length, 1); assert.equal(p.dialogs(), 0);
    }
});

test('rejected transport is not repeated and never converted into a save', async () => {
    const p = app(() => { throw Error('Synthetic response loss'); });
    await assert.rejects(p.fetch(), /Synthetic response loss/); assert.equal(p.calls.length, 1);
    assert.equal(p.dialogs(), 0);
});
