const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

// No network primitive is available to the scripts. Every permitted response is
// supplied by the individual case through this synthetic request boundary.
const sharedSource = fs.readFileSync(path.join(__dirname, '../static/account-parent-auth-v1.js'), 'utf8');
const response = (status, value) => new Response(JSON.stringify(value), {
    status, headers: {'Content-Type': 'application/json'},
});
const token = (prefix, character = 'a') => prefix + character.repeat(64);
const deferred = () => {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return {promise, resolve};
};
async function until(predicate) {
    const deadline = Date.now() + 3000;
    while (!predicate() && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 1));
    }
    assert.ok(predicate(), 'Synthetic async barrier did not arrive');
}

function app(reply, storageOptions = {}) {
    const calls = [], dialogs = [], timers = new Set(), listeners = new Map();
    const storageWrites = [], storageReads = [], values = new Map();
    class Element {
        constructor(tag) {
            this.tagName = tag; this.children = []; this.events = new Map();
            this.style = {}; this.dataset = {}; this.value = ''; this.checked = false;
            this.disabled = false; this.isConnected = true; this.textContent = '';
        }
        append(...children) { this.children.push(...children); }
        appendChild(child) { this.append(child); return child; }
        replaceChildren(...children) { this.children = children; }
        set innerHTML(value) { this.html = value; this.children = []; }
        get innerHTML() { return this.html || ''; }
        querySelector(selector) { return this.selectors?.get(selector) || null; }
        querySelectorAll(selector) { return this.queries?.get(selector) || []; }
        contains(child) { return this.children.includes(child); }
        setAttribute(name, value) { this[name] = value; }
        getAttribute(name) { return this[name] ?? null; }
        addEventListener(name, callback) {
            if (!this.events.has(name)) this.events.set(name, []);
            this.events.get(name).push(callback);
        }
        removeEventListener(name, callback) {
            this.events.set(name, (this.events.get(name) || []).filter(item => item !== callback));
        }
        dispatch(name, extra = {}) {
            for (const callback of this.events.get(name) || []) callback({
                target: this, currentTarget: this, preventDefault() {}, ...extra,
            });
        }
        showModal() { dialogs.push(this); }
        focus() {}
        remove() { this.removed = true; this.isConnected = false; }
    }
    const body = new Element('body');
    const elements = new Map();
    const storage = {
        get length() { return values.size; },
        key(index) { return [...values.keys()][index] ?? null; },
        getItem(key) {
            storageReads.push(String(key));
            if (storageOptions.getThrows) throw Error('Synthetic storage read denied');
            const value = values.get(String(key)) ?? null;
            return storageOptions.corruptReadback && value !== null ? '{corrupt' : value;
        },
        setItem(key, value) {
            if (storageOptions.setThrows) throw Error('Synthetic storage quota exceeded');
            storageWrites.push([String(key), String(value)]);
            if (!storageOptions.discardWrites) values.set(String(key), String(value));
        },
        removeItem(key) { values.delete(String(key)); },
        clear() { values.clear(); },
    };
    const document = {
        body, createElement: tag => new Element(tag),
        getElementById: id => elements.get(id) || null,
        querySelector: selector => elements.get(selector) || null,
        querySelectorAll: () => [],
        addEventListener(name, callback) {
            if (!listeners.has('document:' + name)) listeners.set('document:' + name, []);
            listeners.get('document:' + name).push(callback);
        },
    };
    const window = {
        location: {origin: 'https://server.test'}, sessionStorage: storage,
        confirm: () => true,
        addEventListener(name, callback) {
            if (!listeners.has(name)) listeners.set(name, []);
            listeners.get(name).push(callback);
        },
        fetch: async input => {
            assert.ok(input instanceof Request, 'Shared helper must freeze a real Request');
            const request = input;
            const call = {
                request, url: request.url, method: request.method,
                body: await request.clone().text(), signal: request.signal,
                password: request.headers.get('X-Account-Password'),
                intent: request.headers.get('X-Parent-Intent'),
            };
            calls.push(call);
            if (!reply) throw Error('Unexpected synthetic request');
            return reply(call, calls.length);
        },
    };
    class BrowserRequest extends Request {
        constructor(input, options) {
            super(typeof input === 'string' ? new URL(input, window.location.origin).href : input, options);
        }
    }
    const context = vm.createContext({
        window, document, sessionStorage: storage, Request: BrowserRequest, Response, Headers, URL,
        Promise, TextEncoder, TextDecoder, AbortController, DOMException, btoa, atob,
        console, crypto: require('node:crypto').webcrypto,
        setTimeout(callback) { timers.add(callback); return callback; },
        clearTimeout(callback) { timers.delete(callback); },
    });
    vm.runInContext(sharedSource, context);
    const activeDialogs = () => dialogs.filter(dialog => !dialog.removed);
    async function respond(value) {
        await until(() => activeDialogs().length > 0);
        const dialog = activeDialogs().at(-1), form = dialog.children[0];
        const input = form.children.find(child => child.tagName === 'label').children[0];
        if (value === 'escape') dialog.dispatch('cancel');
        else if (value === null) form.children.find(child => child.textContent === 'Cancel').dispatch('click');
        else { input.value = value; form.dispatch('submit'); }
        assert.equal(input.value, ''); assert.equal(dialog.removed, true);
    }
    return {
        window, context, document, Element, elements, calls, dialogs, activeDialogs,
        respond, timers, storage, storageWrites, storageReads, storageValues: values,
        pagehide() { for (const callback of listeners.get('pagehide') || []) callback(); },
    };
}

const actor = 'account-a';
const basePath = '/account';
const createURL = 'https://server.test/account/api/profiles';
const ref = token('pcr_');
const intent = token('pcc_');
const challenge = (overrides = {}) => response(403, {
    code: 'parent_authorization_required', error: 'Confirm the account password.',
    parentIntent: intent, creationOperation: ref, ...overrides,
});
const storageKey = (account = actor, prefix = basePath) =>
    `mediastorm.profileCreation.v1:${encodeURIComponent(account)}:${encodeURIComponent(prefix)}`;
const entry = (reference = ref, ownerID = actor) => ({ref: reference, ownerID, createdAt: '2026-11-17T12:00:00.000Z'});
const container = (entries, account = actor, prefix = basePath) => ({
    version: 1, actorAccountID: account, basePath: prefix, entries,
});
function client(p, account = actor, prefix = basePath) {
    return p.window.createProfileCreationClient({actorAccountID: account, basePath: prefix});
}
function create(p, store, options = {}, action = {}) {
    const controller = new AbortController();
    return p.window.parentProfileCreationFetch(store, createURL, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({accountId: actor, name: 'Synthetic child 雪', color: '#123456', isKidsProfile: true}),
        ...options,
    }, {ownerID: actor, signal: controller.signal, isCurrent: () => true, ...action});
}
const plain = value => JSON.parse(JSON.stringify(value));

test('creation freezes original request bytes and acknowledges a recoverable locator before the only password retry', async () => {
    let acknowledged = false, retryNotified = false;
    const p = app(call => {
        if (!call.intent) return challenge();
        assert.equal(acknowledged, true); assert.equal(retryNotified, true);
        assert.equal(store.list()[0].ref, ref);
        return response(503, {code: 'parent_confirmation_outcome_unknown'});
    });
    const store = client(p), controller = new AbortController();
    const raw = ' {"accountId":"account-a", "name":"Synthetic child 雪", "isKidsProfile":true,"color":""} ';
    const pending = create(p, store, {body: raw, signal: controller.signal}, {
        signal: controller.signal,
        onLocatorAcknowledged(value) {
            assert.equal(value, ref); assert.equal(store.list()[0].ref, ref);
            assert.equal(p.activeDialogs().length, 0); acknowledged = true;
        },
        onRetryDispatched(value) { assert.equal(value, ref); retryNotified = true; },
    });
    await until(() => p.activeDialogs().length === 1);
    assert.equal(acknowledged, true); assert.equal(retryNotified, false);
    await p.respond('  synthetic password 雪  ');
    const result = await pending;
    assert.equal(result.status, 503); assert.equal(p.calls.length, 2);
    assert.equal(p.calls[0].url, p.calls[1].url);
    assert.equal(p.calls[0].method, 'POST'); assert.equal(p.calls[1].method, 'POST');
    assert.equal(p.calls[0].body, raw); assert.equal(p.calls[1].body, raw);
    assert.equal(p.calls[0].password, null); assert.equal(p.calls[0].intent, null);
    assert.equal(p.calls[1].intent, intent);
    assert.equal(p.calls[0].request.headers.get('X-Profile-Creation-Operation'), null);
    assert.equal(p.calls[1].request.headers.get('X-Profile-Creation-Operation'), ref);
    assert.equal(p.calls[1].request.headers.get('X-Profile-Creation-Operation'), ref);
    assert.equal(Buffer.from(p.calls[1].password.slice(7), 'base64').toString(), '  synthetic password 雪  ');
    assert.equal(p.window.profileCreationResponseIsQualified(result), true);
    assert.equal(p.window.profileCreationResponseOperation(result), ref);
    assert.equal(store.list().length, 1);
    controller.abort();
    assert.equal(p.calls[0].signal.aborted, true); assert.equal(p.calls[1].signal.aborted, true);
    for (const [, serialized] of p.storageWrites) {
        for (const secret of [intent, 'Synthetic child', '#123456', 'synthetic password', 'isKidsProfile']) {
            assert.equal(serialized.includes(secret), false, secret);
        }
    }
});

test('client construction and ordinary legacy success do not touch storage or acquire a confirmation', async () => {
    const p = app(() => response(200, {id: 'legacy-child'})), store = client(p);
    assert.equal(p.storageReads.length, 0); assert.equal(p.storageWrites.length, 0);
    const result = await create(p, store, {body: JSON.stringify({name: 'Adult', isKidsProfile: false})});
    assert.equal(result.status, 200); assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    assert.equal(p.window.profileCreationResponseIsQualified(result), false);
    assert.equal(p.window.profileCreationResponseOperation(result), null);
    assert.equal(p.storageReads.length, 0); assert.equal(p.storageWrites.length, 0);
});

test('creation never consumes other action intents or malformed locators and never falls back to legacy confirmation', async () => {
    for (const overrides of [
        {parentIntent: token('pmi_')}, {parentIntent: token('pkf_')},
        {parentIntent: token('pli_')}, {parentIntent: token('prp_')},
        {parentIntent: null}, {parentIntent: undefined}, {parentIntent: 'pcc_bad'},
        {parentIntent: intent + '\n'}, {parentIntent: intent + '\r\n'},
        {creationOperation: token('pcc_')}, {creationOperation: null}, {creationOperation: undefined},
        {creationOperation: 'pcr_bad'}, {creationOperation: ref + '\n'},
    ]) {
        const p = app(() => challenge(overrides)), store = client(p);
        assert.equal((await create(p, store)).status, 403);
        assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
        assert.equal(p.storageWrites.length, 0);
    }
});

test('legacy no-intent creation confirmation reuses the old shared prompt without locator storage', async () => {
    const p = app(call => call.password ? response(200, savedProfile()) : response(403, {
        code: 'parent_authorization_required', error: 'Confirm the account password.',
    })), store = client(p);
    const first = create(p, store), second = create(p, store);
    await until(() => p.calls.length === 2 && p.activeDialogs().length === 1);
    await p.respond('synthetic legacy password'); await Promise.all([first, second]);
    assert.equal(p.dialogs.length, 1); assert.equal(p.calls.length, 4);
    assert.equal(p.calls.filter(call => call.password).length, 2);
    assert.equal(p.calls.every(call => call.intent === null), true);
    assert.equal(p.storageWrites.length, 0); assert.equal(p.storageReads.length, 0);
});

test('Cancel Escape and prompt timeout retain a locator without dispatching a password or any Read', async () => {
    for (const choice of [null, 'escape', 'timeout']) {
        const p = app(() => challenge()), store = client(p), pending = create(p, store);
        await until(() => p.activeDialogs().length === 1);
        if (choice === 'timeout') [...p.timers][0](); else await p.respond(choice);
        const result = await pending;
        assert.equal(result.status, 403); assert.equal(p.calls.length, 1);
        assert.equal(p.window.profileCreationResponseOperation(result), ref);
        assert.equal(p.window.profileCreationResponseIsQualified(result), false);
        assert.equal(store.list()[0].ref, ref);
    }
});

test('storage denial discarded writes and corrupt readback prevent the password retry', async () => {
    for (const options of [{getThrows: true}, {setThrows: true}, {discardWrites: true}, {corruptReadback: true}]) {
        const p = app(() => challenge(), options), store = client(p);
        await assert.rejects(create(p, store));
        assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
        assert.equal(p.calls[0].password, null);
    }
});

test('the bounded store refuses a ninth pending locator without dropping earlier operations', async () => {
    const p = app(() => challenge()), store = client(p);
    const entries = 'bcdef012'.split('').map(character => entry(token('pcr_', character)));
    p.storageValues.set(storageKey(), JSON.stringify(container(entries)));
    await assert.rejects(create(p, store));
    assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    assert.deepEqual(plain(store.list()), entries);
});

test('stored state is namespaced by exact actor and rendered full base path and rejects oversized or poisoned records', () => {
    const p = app(), first = client(p), otherActor = client(p, 'account-b'), otherPath = client(p, actor, '/proxy/account');
    p.storageValues.set(storageKey(), JSON.stringify(container([entry()])));
    assert.deepEqual(plain(first.list()), [entry()]);
    assert.deepEqual(plain(otherActor.list()), []); assert.deepEqual(plain(otherPath.list()), []);
    for (const malformed of [
        '{bad', JSON.stringify(container([entry()], 'other-account')),
        JSON.stringify({...container([entry()]), password: 'synthetic-secret'}),
        JSON.stringify(container([{...entry(), parentIntent: intent}])),
        JSON.stringify(container([entry(token('pcc_'))])),
        JSON.stringify(container([entry(ref + '\r\n')])),
        ' '.repeat(8193),
    ]) {
        p.storageValues.set(storageKey(), malformed);
        assert.throws(() => first.list());
    }
    assert.equal(p.calls.length, 0);
});

test('creation namespace rejects normalized traversal or protocol-relative management aliases', () => {
    const p = app();
    for (const invalid of ['/proxy/../account', '/proxy/%2e%2e/account', '//server.test/account',
        '/account?other=1', '/account#other', 'https://server.test/account', '/account/']) {
        assert.throws(() => client(p, actor, invalid), invalid);
    }
    assert.equal(p.storageReads.length, 0); assert.equal(p.storageWrites.length, 0);
    assert.equal(p.calls.length, 0);
});

test('namespace validation rejects path normalization or authority aliases rather than silently switching management roots', () => {
    const p = app();
    for (const prefix of ['/proxy/../account', '/proxy/%2e%2e/account', '//server.test/account',
        'https://other.test/account', '/account?other=1', '/account#fragment', '/account/']) {
        assert.throws(() => client(p, actor, prefix), prefix);
    }
    assert.equal(p.calls.length, 0); assert.equal(p.storageWrites.length, 0);
});

test('forget and resolve remove only the selected locator and never issue server requests', () => {
    const p = app(), store = client(p), second = token('pcr_', 'b');
    p.storageValues.set(storageKey(), JSON.stringify(container([entry(), entry(second)])));
    assert.equal(store.forget(ref), true); assert.equal(store.forget(ref), false);
    assert.deepEqual(plain(store.list()), [entry(second)]);
    assert.equal(store.resolve(second), true); assert.deepEqual(plain(store.list()), []);
    assert.equal(p.calls.length, 0);
});

test('storage must still acknowledge the locator when confirmation completes', async () => {
    const options = {}, p = app(() => challenge(), options), store = client(p);
    const pending = create(p, store), rejected = assert.rejects(pending);
    await until(() => p.activeDialogs().length === 1);
    options.corruptReadback = true;
    await p.respond('synthetic password'); await rejected;
    assert.equal(p.calls.length, 1);
});

test('retirement inside the acknowledged-locator callback cannot open a stale password prompt', async () => {
    const p = app(() => challenge()), store = client(p); let current = true;
    const result = await create(p, store, {}, {
        isCurrent: () => current,
        onLocatorAcknowledged(value) { assert.equal(value, ref); current = false; },
    });
    assert.equal(result.status, 403); assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    assert.equal(store.list()[0].ref, ref);
});

test('late challenge JSON is checked against the original action before storage or confirmation', async () => {
    const delayed = deferred(); let parsing = false, current = true;
    const p = app(() => {
        const first = challenge();
        first.clone = () => ({json() { parsing = true; return delayed.promise; }});
        return first;
    }), store = client(p);
    const pending = create(p, store, {}, {isCurrent: () => current});
    const rejected = assert.rejects(pending, error => error.name === 'AbortError');
    await until(() => parsing); current = false;
    delayed.resolve({code: 'parent_authorization_required', error: 'Synthetic confirmation', parentIntent: intent, creationOperation: ref});
    await rejected; assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    assert.equal(p.storageWrites.length, 0);
});

test('form edits scope retirement pagehide and aborted signals prevent a stale creation retry', async () => {
    for (const reason of ['form', 'account', 'ABA', 'page', 'signal']) {
        const p = app(() => challenge()), store = client(p), controller = new AbortController();
        let generation = 1;
        const pending = create(p, store, {}, {signal: controller.signal, isCurrent: () => generation === 1});
        await until(() => p.activeDialogs().length === 1);
        if (reason === 'page') p.pagehide();
        else if (reason === 'signal') controller.abort();
        else { generation++; await p.respond('synthetic password'); }
        const result = await pending;
        assert.equal(result.status, 403); assert.equal(p.window.profileCreationResponseOperation(result), ref);
        assert.equal(p.window.profileCreationResponseIsQualified(result), false);
        assert.equal(p.calls.length, 1); assert.equal(p.activeDialogs().length, 0);
        assert.equal(store.list()[0].ref, ref);
    }
});

test('unknown changed unavailable and transport loss never automatically create or read again', async () => {
    for (const outcome of [
        [503, 'parent_confirmation_outcome_unknown'], [503, 'parent_confirmation_unavailable'],
        [409, 'parent_confirmation_changed'], [403, 'parent_authorization_required'],
        [429, 'rate_limited'], null,
    ]) {
        const p = app(call => {
            if (!call.intent) return challenge();
            if (!outcome) throw Error('Synthetic retry response lost');
            return response(outcome[0], {code: outcome[1], parentIntent: intent, creationOperation: ref});
        }), store = client(p), pending = create(p, store);
        const settled = outcome ? pending : assert.rejects(pending, /Synthetic retry response lost/);
        await p.respond('synthetic password'); await settled;
        assert.equal(p.calls.length, 2); assert.equal(p.dialogs.length, 1);
        assert.equal(store.list()[0].ref, ref);
    }
});

test('explicit Read is one bodyless management request with no password intent locator header or confirmation', async () => {
    const p = app(() => response(200, {outcome: 'pending', usersWriteBlocked: true, settingsPendingUncertainty: true}));
    const store = client(p); p.storageValues.set(storageKey(), JSON.stringify(container([entry()])));
    const controller = new AbortController();
    await p.window.profileCreationResultReadFetch(store, ref, {signal: controller.signal, isCurrent: () => true});
    assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    const {request, body} = p.calls[0];
    assert.equal(request.url, 'https://server.test/account/api/profiles/creation-operations/' + ref + '/read');
    assert.equal(request.method, 'POST'); assert.equal(request.body, null); assert.equal(body, '');
    assert.equal(request.cache, 'no-store'); assert.equal(request.redirect, 'error');
    assert.equal(request.credentials, 'same-origin'); assert.equal(request.mode, 'same-origin');
    for (const header of ['X-Account-Password', 'X-Parent-Intent', 'X-Profile-Creation-Operation']) {
        assert.equal(request.headers.get(header), null);
    }
    assert.equal(store.list().length, 1);
});

test('explicit Read preserves errors without a password prompt and suppresses late retired responses', async () => {
    for (const status of [401, 403, 404, 409, 429, 503]) {
        const p = app(() => response(status, {code: 'parent_authorization_required', parentIntent: intent})), store = client(p);
        p.storageValues.set(storageKey(), JSON.stringify(container([entry()])));
        const result = await p.window.profileCreationResultReadFetch(store, ref, {signal: new AbortController().signal, isCurrent: () => true});
        assert.equal(result.status, status); assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0);
    }
    const delayed = deferred(), p = app(() => delayed.promise), store = client(p);
    p.storageValues.set(storageKey(), JSON.stringify(container([entry()])));
    let current = true;
    const pending = p.window.profileCreationResultReadFetch(store, ref, {signal: new AbortController().signal, isCurrent: () => current});
    const rejected = assert.rejects(pending, error => error.name === 'AbortError');
    await until(() => p.calls.length === 1); current = false; delayed.resolve(response(200, {}));
    await rejected; assert.equal(store.list().length, 1);
});

function savedProfile() {
    return {id: 'child-a', name: 'Saved child 雪', hasPin: false, hasIcon: false, isKidsProfile: true,
        allowShareLinks: false, allowLiveTakeover: false, allowRecordingInterruption: false,
        activityPrivacy: 'private', createdAt: '2026-11-17T12:00:00Z', updatedAt: '2026-11-17T12:00:00Z',
        accountId: actor, color: '#123456'};
}
test('profile projection accepts required real DTO fields strips ordinary extras and rejects credential or malformed fields', () => {
    const p = app(), decode = p.window.decodeProfileCreationProfile;
    const input = {...savedProfile(), ignoredFutureField: 'not rendered'};
    const output = plain(decode(input));
    assert.equal(output.id, 'child-a'); assert.equal(output.ignoredFutureField, undefined);
    for (const date of ['2026-11-17T12:00:00.123456789Z', '2026-11-17T14:00:00+02:00']) {
        assert.equal(decode({...savedProfile(), createdAt: date}).createdAt, date);
    }
    for (const key of ['id', 'name', 'hasPin', 'hasIcon', 'isKidsProfile', 'allowShareLinks',
        'allowLiveTakeover', 'allowRecordingInterruption', 'activityPrivacy', 'createdAt', 'updatedAt']) {
        const invalid = savedProfile(); delete invalid[key]; assert.throws(() => decode(invalid), key);
    }
    for (const change of [{hasPin: null}, {allowLiveTakeover: 'false'}, {createdAt: 'not-a-date'},
        {createdAt: '2026'}, {updatedAt: '2026-11-17'},
        {createdAt: '2026-02-30T12:00:00Z'},
        {createdAt: '2026'}, {updatedAt: '2026-11-17'},
        {kidsAllowedLists: [3]}, {color: null}, {pinHash: 'synthetic hash'}]) {
        assert.throws(() => decode({...savedProfile(), ...change}));
    }
});

test('Read decoder distinguishes qualified absence pending replacement and creation without treating unavailable as absence', () => {
    const p = app(), decode = p.window.decodeProfileCreationResult;
    for (const outcome of ['pending', 'not_created', 'changed_or_replaced']) {
        const input = {outcome, usersWriteBlocked: true, settingsPendingUncertainty: true};
        assert.equal(decode(200, input).outcome, outcome);
        assert.throws(() => decode(200, {...input, profile: savedProfile()}));
    }
    const created = {outcome: 'created', usersWriteBlocked: false, settingsPendingUncertainty: false, profile: savedProfile()};
    assert.equal(decode(200, created).profile.id, 'child-a');
    const withoutProfile = {...created}; delete withoutProfile.profile;
    assert.equal(decode(200, withoutProfile).outcome, 'created');
    assert.equal(decode(404, {outcome: 'unqualified_or_missing', usersWriteBlocked: true, settingsPendingUncertainty: true}).outcome, 'unqualified_or_missing');
    for (const [status, input] of [
        [503, {outcome: 'not_created', usersWriteBlocked: false, settingsPendingUncertainty: false}],
        [200, {outcome: 'created', usersWriteBlocked: false}],
        [200, {outcome: 'made_up', usersWriteBlocked: true, settingsPendingUncertainty: true}],
        [404, created], [200, {...created, usersWriteBlocked: 'false'}],
    ]) assert.throws(() => decode(status, input));
});

// Execute the actual contiguous creation region, including its real state and
// pagehide registration. Only the server-rendered actor value is substituted.
// Surrounding unrelated UI services are observed stubs, not reimplementations.
function page(kind, reply, options = {}) {
    const p = app(reply, options.storage || {});
    const html = fs.readFileSync(path.join(__dirname, '../admin_templates', kind + '.html'), 'utf8');
    const start = html.indexOf('const profileCreationActorID = ');
    const end = html.indexOf(kind === 'accounts' ? '\nfunction showLivePermissions(' : '\nfunction renderReview(', start);
    assert.ok(start >= 0 && end > start, 'Actual creation region must remain identifiable');
    let script = html.slice(start, end);
    if (kind === 'onboarding') {
        assert.equal(script.split('{{json .AccountID}}').length, 2);
        script = script.replace('{{json .AccountID}}', JSON.stringify(actor));
    }
    const form = new p.Element('form'), fields = {};
    for (const [name, value] of Object.entries({
        name: 'Draft child 雪', accountId: actor, color: '#123456',
        iconUrl: 'https://synthetic.invalid/icon.png', pin: '4826', isKids: '',
    })) {
        const input = new p.Element(name === 'accountId' ? 'select' : 'input');
        input.value = value; fields[name] = input; form.append(input);
    }
    fields.isKids.checked = true;
    form.elements = fields;
    form.queries = new Map([['input,select', Object.values(fields)]]);
    const panel = new p.Element('section'), status = new p.Element('p'), rows = new p.Element('div');
    panel.selectors = new Map([['[data-profile-creation-status]', status], ['[data-creation-records]', rows]]);
    const overlay = new p.Element('div'); overlay.style.display = 'flex';
    p.elements.set('profile-creation-results', panel); p.elements.set('modal-overlay', overlay);
    p.elements.set('#modal-content form[data-profile-create]', form);
    p.elements.set('form[data-profile-create]', form);
    for (const [id, name] of Object.entries({profileName: 'name', profileAccountId: 'accountId',
        profileColor: 'color', profilePin: 'pin', profileKids: 'isKids'})) p.elements.set(id, fields[name]);
    const submit = new p.Element('button');
    p.document.querySelectorAll = selector => selector.includes('button[type="submit"]') ? [submit] :
        selector.includes('form[data-profile-create] input') ? Object.values(fields) : [];
    const effects = {refresh: 0, toasts: [], editorRetired: 0};
    Object.assign(p.context, {
        accountId: actor, basePath, isAdmin: options.isAdmin ?? true,
        accounts: [{id: actor, username: 'Synthetic account'}], profiles: [{id: 'older-profile'}],
        activeStep: 4, profileUsersWriteBlocked: false,
        fetch: (...args) => p.window.fetch(...args),
        getSelectedProfileColor: () => fields.color.value,
        showToast: (...args) => effects.toasts.push(args),
        retireProfileEditorAction: () => effects.editorRetired++,
        loadData: () => { effects.refresh++; },
        loadAccountsAndProfiles: async () => { effects.refresh++; },
    });
    vm.runInContext(script, p.context);
    if (kind === 'accounts') {
        const hideStart = html.indexOf('function hideModal() {');
        const hideEnd = html.indexOf('\n}', hideStart) + 2;
        assert.ok(hideStart >= 0 && hideEnd > hideStart);
        vm.runInContext(html.slice(hideStart, hideEnd), p.context);
    }
    p.context.bindProfileCreationForm(form);
    return {
        ...p, kind, form, fields, panel, status, rows, submit, overlay, effects, html,
        create: () => p.context.createProfile({target: form, preventDefault() {}}),
        read: reference => p.context.readProfileCreationResult(reference),
        evaluate: expression => vm.runInContext(expression, p.context),
        seed(reference = ref) { p.storageValues.set(storageKey(), JSON.stringify(container([entry(reference)]))); },
    };
}

for (const kind of ['accounts', 'onboarding']) {
    test(kind + ' actual creation double submit sends only one original request and qualified success defers optional writes', async () => {
        const p = page(kind, call => call.intent ? response(200, {...savedProfile(), creationOperation: ref}) : challenge());
        const pending = p.create(); await until(() => p.activeDialogs().length === 1);
        await p.create(); assert.equal(p.calls.length, 1);
        await p.respond('synthetic password'); await pending;
        assert.equal(p.calls.length, 2); assert.equal(p.effects.refresh, 1);
        assert.deepEqual(plain(p.evaluate('getProfileCreationStore().list()')), []);
        assert.equal(p.calls.some(call => /\/icon\?|\/pin\?/.test(call.url)), false);
        assert.equal(p.evaluate('profileCreationResults.get(' + JSON.stringify(ref) + ').outcome'), 'created');
        if (kind === 'accounts') assert.equal(p.overlay.style.display, 'none');
        else { assert.equal(p.fields.name.value, ''); assert.equal(p.fields.pin.value, ''); }
    });

    test(kind + ' actual prompt cancellation retains its panel check without closing resetting or following up', async () => {
        const p = page(kind, () => challenge()), pending = p.create();
        await p.respond(null); await pending;
        assert.equal(p.calls.length, 1); assert.equal(p.effects.refresh, 0);
        assert.equal(p.fields.name.value, 'Draft child 雪'); assert.equal(p.fields.pin.value, '4826');
        assert.equal(p.overlay.style.display, 'flex');
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
        assert.equal(p.evaluate('profileCreationUsersWriteBlocked'), false);
        assert.equal(p.submit.disabled, true); assert.equal(p.rows.children.length, 1);
    });

    test(kind + ' actual pagehide retires the open prompt without dropping its acknowledged check', async () => {
        const p = page(kind, () => challenge()), pending = p.create();
        await until(() => p.activeDialogs().length === 1); p.pagehide(); await pending;
        assert.equal(p.calls.length, 1); assert.equal(p.activeDialogs().length, 0);
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
        assert.equal(p.effects.refresh, 0); assert.equal(p.fields.name.value, 'Draft child 雪');
        assert.equal(p.submit.disabled, true);
    });

    test(kind + ' actual legacy adult creation keeps its original optional cosmetic or PIN followup', async () => {
        const p = page(kind, call => {
            if (call.method === 'POST') return response(200, savedProfile());
            assert.match(call.url, kind === 'accounts' ? /\/profiles\/icon\?/ : /\/profiles\/pin\?/);
            return response(200, {});
        });
        p.fields.isKids.checked = false;
        await p.create(); assert.equal(p.calls.length, 2); assert.equal(p.dialogs.length, 0);
        assert.equal(p.calls[0].intent, null); assert.equal(p.calls[1].intent, null);
        assert.deepEqual(JSON.parse(p.calls[1].body), kind === 'accounts' ?
            {iconUrl: 'https://synthetic.invalid/icon.png'} : {pin: '4826'});
        assert.equal(p.effects.refresh, 1);
    });

    test(kind + ' actual retired creation cannot finish a late retry or clear and close newer input', async () => {
        const delayed = deferred();
        const p = page(kind, call => call.intent ? delayed.promise : challenge());
        const pending = p.create(); await p.respond('synthetic password');
        await until(() => p.calls.length === 2);
        p.fields.name.value = 'New draft'; p.form.dispatch('input');
        p.fields.name.value = 'Draft child 雪'; p.form.dispatch('input');
        delayed.resolve(response(200, {...savedProfile(), creationOperation: ref}));
        await pending;
        assert.equal(p.effects.refresh, 0); assert.equal(p.fields.name.value, 'Draft child 雪');
        assert.equal(p.fields.pin.value, '4826'); assert.equal(p.overlay.style.display, 'flex');
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
        assert.equal(p.evaluate('profileCreationResults.size'), 0);
    });

    test(kind + ' actual success rejects mismatched or missing returned operation before resolving or changing its form', async () => {
        for (const returned of [token('pcr_', 'b'), undefined]) {
            const p = page(kind, call => {
                const saved = savedProfile();
                if (returned !== undefined) saved.creationOperation = returned;
                return call.intent ? response(200, saved) : challenge();
            });
            const pending = p.create(); await p.respond('synthetic password'); await pending;
            assert.equal(p.effects.refresh, 0); assert.equal(p.fields.name.value, 'Draft child 雪');
            assert.equal(p.fields.pin.value, '4826'); assert.equal(p.overlay.style.display, 'flex');
            assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
            assert.equal(p.evaluate('profileCreationResults.size'), 0);
        }
    });

    test(kind + ' actual late success JSON cannot overwrite a new draft after input ABA', async () => {
        const delayedJSON = deferred(); let parsing = false;
        const p = page(kind, call => {
            if (!call.intent) return challenge();
            const result = response(200, {...savedProfile(), creationOperation: ref});
            result.json = () => { parsing = true; return delayedJSON.promise; }; return result;
        });
        const pending = p.create(); await p.respond('synthetic password');
        await until(() => parsing);
        p.fields.accountId.value = 'account-b'; p.form.dispatch('change');
        p.fields.accountId.value = actor; p.form.dispatch('change');
        delayedJSON.resolve({...savedProfile(), creationOperation: ref}); await pending;
        assert.equal(p.effects.refresh, 0); assert.equal(p.evaluate('profileCreationResults.size'), 0);
        assert.equal(p.fields.name.value, 'Draft child 雪');
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
    });

    test(kind + ' actual explicit Read adopts only panel state and leaves all draft arrays and optional fields unchanged', async () => {
        const p = page(kind, call => {
            assert.match(call.url, /\/creation-operations\/pcr_[a-f0-9]{64}\/read$/);
            return response(200, {outcome: 'created', profile: {...savedProfile(), name: '<img src=x onerror=alert(1)>'},
                usersWriteBlocked: true, settingsPendingUncertainty: true});
        });
        p.seed(); const oldProfiles = p.context.profiles, oldAccounts = p.context.accounts;
        await p.read(ref);
        assert.equal(p.calls.length, 1); assert.equal(p.dialogs.length, 0); assert.equal(p.effects.refresh, 0);
        assert.equal(p.context.profiles, oldProfiles); assert.equal(p.context.accounts, oldAccounts);
        assert.equal(p.fields.name.value, 'Draft child 雪'); assert.equal(p.fields.pin.value, '4826');
        assert.equal(p.fields.isKids.checked, true); assert.equal(p.overlay.style.display, 'flex');
        assert.equal(p.evaluate('profileCreationUsersWriteBlocked'), true);
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 0);
        assert.equal(p.rows.children.length, 1);
        assert.match(p.rows.children[0].children[0].textContent, /<img src=x onerror=alert\(1\)>/);
        assert.equal(p.rows.children[0].children[0].innerHTML, '');
    });

    test(kind + ' actual pending missing replaced or malformed Read keeps its check and never sends another Create', async () => {
        for (const [status, value] of [
            [200, {outcome: 'pending', usersWriteBlocked: true, settingsPendingUncertainty: true}],
            [200, {outcome: 'changed_or_replaced', usersWriteBlocked: true, settingsPendingUncertainty: true}],
            [404, {outcome: 'unqualified_or_missing', usersWriteBlocked: true, settingsPendingUncertainty: true}],
            [503, {code: 'parent_confirmation_unavailable'}],
            [200, {outcome: 'created', usersWriteBlocked: false}],
        ]) {
            const p = page(kind, () => response(status, value)); p.seed(); await p.read(ref);
            assert.equal(p.calls.length, 1); assert.equal(p.calls[0].method, 'POST');
            assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
            assert.equal(p.effects.refresh, 0); assert.equal(p.fields.name.value, 'Draft child 雪');
        }
    });

    test(kind + ' actual late Read JSON never resolves a retained check after new form edits', async () => {
        const delayedJSON = deferred(); let parsing = false;
        const p = page(kind, () => {
            const result = response(200, {}); result.json = () => { parsing = true; return delayedJSON.promise; }; return result;
        });
        p.seed(); const pending = p.read(ref); await until(() => parsing);
        p.fields.name.value = 'New draft after Read'; p.form.dispatch('input');
        delayedJSON.resolve({outcome: 'not_created', usersWriteBlocked: false, settingsPendingUncertainty: false});
        await pending;
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
        assert.equal(p.evaluate('profileCreationResults.size'), 0);
        assert.equal(p.fields.name.value, 'New draft after Read'); assert.equal(p.effects.refresh, 0);
    });

    test(kind + ' actual explicit forget only discards local correlation without cancelling or creating remotely', () => {
        const p = page(kind, () => { throw Error('Forget must not use fetch'); }); p.seed();
        p.context.forgetProfileCreationResult(ref);
        assert.equal(p.calls.length, 0); assert.equal(p.evaluate('getProfileCreationStore().list().length'), 0);
        assert.equal(p.fields.name.value, 'Draft child 雪'); assert.equal(p.effects.refresh, 0);
    });

    test(kind + ' fresh-page retained check blocks another Create until explicit local discard', async () => {
        const p = page(kind, () => { throw Error('Retained operation must prevent another Create'); });
        p.seed(); p.context.renderProfileCreationResults();
        await p.create();
        assert.equal(p.calls.length, 0); assert.equal(p.submit.disabled, true);
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 1);
        p.context.forgetProfileCreationResult(ref);
        assert.equal(p.calls.length, 0); assert.equal(p.submit.disabled, false);
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 0);
    });

    test(kind + ' reading B with false server flags cannot clear a possibly sent A or permit another Create', async () => {
        const otherRef = token('pcr_', 'b');
        const p = page(kind, call => {
            if (call.url.endsWith('/' + otherRef + '/read')) {
                return response(200, {outcome: 'not_created', usersWriteBlocked: false, settingsPendingUncertainty: false});
            }
            return call.intent ? response(503, {code: 'parent_confirmation_outcome_unknown'}) : challenge();
        });
        const pending = p.create(); await p.respond('synthetic password'); await pending;
        assert.equal(p.evaluate('profileCreationUncertainOperations.has(' + JSON.stringify(ref) + ')'), true);
        p.storageValues.set(storageKey(), JSON.stringify(container([entry(), entry(otherRef)])));
        await p.read(otherRef);
        assert.equal(p.evaluate('profileCreationUsersWriteBlocked'), false);
        assert.equal(p.evaluate('profileCreationUncertainOperations.has(' + JSON.stringify(ref) + ')'), true);
        assert.equal(p.evaluate('profileCreationUncertainOperations.has(' + JSON.stringify(otherRef) + ')'), false);
        assert.deepEqual(plain(p.evaluate('getProfileCreationStore().list().map(value => value.ref)')), [ref]);
        assert.equal(p.submit.disabled, true);
        await p.create(); assert.equal(p.calls.length, 3);
        p.context.forgetProfileCreationResult(otherRef);
        assert.equal(p.evaluate('profileCreationUncertainOperations.has(' + JSON.stringify(ref) + ')'), true);
        assert.equal(p.submit.disabled, true);
        p.context.forgetProfileCreationResult(ref);
        assert.equal(p.evaluate('profileCreationUncertainOperations.size'), 0);
        assert.equal(p.submit.disabled, false);
    });

    test(kind + ' explicit local discard never clears a fresh server write-block observation', async () => {
        const p = page(kind, () => response(200, {outcome: 'pending', usersWriteBlocked: true, settingsPendingUncertainty: true}));
        p.seed(); await p.read(ref);
        p.context.forgetProfileCreationResult(ref);
        assert.equal(p.evaluate('getProfileCreationStore().list().length'), 0);
        assert.equal(p.evaluate('profileCreationUsersWriteBlocked'), true); assert.equal(p.submit.disabled, true);
        await p.create(); assert.equal(p.calls.length, 1);
    });
}

test('actual Accounts refresh started before a new creation cannot replace that creation owner or profile arrays', async () => {
    const gate = deferred();
    const p = page('accounts', async call => {
        await gate.promise;
        return response(200, call.url.endsWith('/accounts') ? {accounts: [{id: 'replacement-owner'}]} : []);
    });
    Object.assign(p.context, {profileDataGeneration: 0, profilePageRetired: false});
    const start = p.html.indexOf('async function loadData() {');
    const end = p.html.indexOf('\nfunction getAccountName(', start);
    assert.ok(start >= 0 && end > start);
    vm.runInContext(p.html.slice(start, end), p.context);
    const oldAccounts = p.context.accounts, oldProfiles = p.context.profiles;
    const pending = p.context.loadData(); await until(() => p.calls.length === 9);
    const action = p.context.captureProfileCreationAction(p.form);
    assert.ok(action); gate.resolve(); await pending;
    assert.equal(p.context.accounts, oldAccounts); assert.equal(p.context.profiles, oldProfiles);
    assert.equal(action.isCurrent(), true); assert.equal(p.fields.name.value, 'Draft child 雪');
    p.context.retireProfileCreationActions();
});

test('actual Onboarding refresh started before a new creation cannot rebuild its household selector', async () => {
    const p = page('onboarding'), accountReply = deferred(), profileReply = deferred();
    let queries = 0;
    Object.assign(p.context, {
        requestJSON(url) { queries++; return url.endsWith('/accounts') ? accountReply.promise : profileReply.promise; },
        renderMasterAdminForm() { throw Error('Stale refresh rendered master form'); },
        renderAccountsSummary() { throw Error('Stale refresh rendered account summary'); },
        loadStatus() { throw Error('Stale refresh loaded status'); },
    });
    const start = p.html.indexOf('let profileCreationLoadGeneration = 0;');
    const end = p.html.indexOf('\nfunction renderAccountsSummary(', start);
    assert.ok(start >= 0 && end > start);
    vm.runInContext(p.html.slice(start, end), p.context);
    const oldAccounts = p.context.accounts, oldProfiles = p.context.profiles;
    p.fields.accountId.innerHTML = 'original options';
    const pending = p.context.loadAccountsAndProfiles(); assert.equal(queries, 2);
    const action = p.context.captureProfileCreationAction(p.form);
    assert.ok(action);
    accountReply.resolve({accounts: [{id: 'replacement-owner'}]}); profileReply.resolve([]); await pending;
    assert.equal(p.context.accounts, oldAccounts); assert.equal(p.context.profiles, oldProfiles);
    assert.equal(p.fields.accountId.innerHTML, 'original options'); assert.equal(action.isCurrent(), true);
    p.context.retireProfileCreationActions();
});

test('actual Onboarding startup preserves user navigation while its initial observations finish', async t => {
    const html = fs.readFileSync(path.join(__dirname, '../admin_templates/onboarding.html'), 'utf8');
    const startupMarker = "document.addEventListener('DOMContentLoaded', async () => {";
    const startupStart = html.indexOf(startupMarker);
    const startupEnd = html.indexOf('\n});', startupStart);
    const stepStart = html.indexOf('function goToStep(index) {');
    const stepEnd = html.indexOf('\nfunction nextStep()', stepStart);
    assert.equal(html.split(startupMarker).length, 2, 'Exactly one actual startup callback is required');
    assert.ok(startupStart >= 0 && startupEnd > startupStart && stepStart >= 0 && stepEnd > stepStart);

    for (const first of ['status', 'profiles']) {
        await t.test(first + ' observation completes first', async () => {
            const p = app(), statusReply = deferred(), profilesReply = deferred();
            const started = [], callbacks = [];
            let retirements = 0;
            const form = new p.Element('form');
            p.elements.set('form[data-profile-create]', form);
            for (const id of ['serviceMode', 'tmdbApiKey', 'tvdbApiKey']) p.elements.set(id, new p.Element('input'));
            const panels = Array.from({length: 6}, (_, index) => {
                const panel = new p.Element('section');
                panel.dataset.panel = String(index); panel.active = index === 0;
                panel.classList = {toggle(name, enabled) {
                    assert.equal(name, 'active'); panel.active = enabled;
                }};
                return panel;
            });
            p.document.querySelectorAll = selector => selector === '.onboarding-panel' ? panels : [];
            p.document.addEventListener = (name, callback) => {
                assert.equal(name, 'DOMContentLoaded'); callbacks.push(callback);
            };
            // Control only surrounding observations/renderers. Both the startup
            // callback and navigation logic below come from the actual template.
            Object.assign(p.context, {
                activeStep: 0, steps: panels.map((_, index) => 'Synthetic step ' + index), settings: {},
                getRawStepDone: () => [], getHighestUnlocked: () => 5,
                bindProfileCreationForm(value) { assert.equal(value, form); },
                renderProfileCreationResults() {}, seedSearchSources() {}, renderProviderFields() {},
                renderSteps() {}, renderReview() { throw Error('Unexpected review navigation'); },
                showToast() { throw Error('Synthetic unlocked navigation must remain available'); },
                retireProfileCreationActions() { retirements++; },
                loadStatus() { started.push('status'); return statusReply.promise; },
                loadAccountsAndProfiles() { started.push('profiles'); return profilesReply.promise; },
            });
            vm.runInContext(html.slice(stepStart, stepEnd), p.context);
            vm.runInContext(html.slice(startupStart, startupEnd + '\n});'.length), p.context);
            assert.equal(callbacks.length, 1);
            const pending = callbacks[0]();
            assert.deepEqual(started, ['status', 'profiles']);
            p.context.goToStep(4);
            assert.equal(p.context.activeStep, 4);
            assert.deepEqual(panels.map(panel => panel.active), [false, false, false, false, true, false]);

            (first === 'status' ? statusReply : profilesReply).resolve();
            await Promise.resolve();
            assert.equal(p.context.activeStep, 4, 'One completed observation must not reset the selected step');
            (first === 'status' ? profilesReply : statusReply).resolve();
            await pending;
            assert.equal(p.context.activeStep, 4, 'Late startup completion must preserve the user-selected Profiles step');
            assert.deepEqual(panels.map(panel => panel.active), [false, false, false, false, true, false]);
            assert.equal(retirements, 1, 'Startup completion must not retire the newer Profiles action');
            assert.equal(p.calls.length, 0); assert.equal(p.dialogs.length, 0);
        });
    }
});
