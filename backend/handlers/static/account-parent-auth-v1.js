// Retry parental-control mutations only after fresh confirmation by the owner.
(() => {
    'use strict';
    const originalFetch = window.fetch.bind(window);
    const passwordHeader = 'X-Account-Password';
    const intentHeader = 'X-Parent-Intent';
    const profileFlagResponses = new WeakSet();
    let confirmation = null;
    let legacyPrompt = null;
    let intentPrompt = null;
    let pageGeneration = 0;

    function passwordPrompt(signal, completed, explanationText) {
        const dialog = document.createElement('dialog');
        dialog.setAttribute('aria-label', 'Confirm account password');
        dialog.style.cssText = 'max-width:26rem;width:calc(100% - 3rem);padding:1.5rem;border:1px solid #444;border-radius:12px;background:#18181b;color:#fafafa;';
        const form = document.createElement('form');
        const title = document.createElement('h2');
        title.textContent = 'Parent confirmation';
        const explanation = document.createElement('p');
        explanation.textContent = explanationText || 'Enter the account password to change parental controls or manage profiles.';
        const label = document.createElement('label');
        label.textContent = 'Account password';
        const input = document.createElement('input');
        input.type = 'password';
        input.autocomplete = 'current-password';
        input.required = true;
        input.style.cssText = 'display:block;box-sizing:border-box;width:100%;margin:0.6rem 0 1rem;padding:0.65rem;border-radius:6px;';
        label.appendChild(input);
        const cancel = document.createElement('button');
        cancel.type = 'button';
        cancel.textContent = 'Cancel';
        const submit = document.createElement('button');
        submit.type = 'submit';
        submit.textContent = 'Confirm';
        submit.style.marginLeft = '0.75rem';
        form.append(title, explanation, label, cancel, submit);
        dialog.appendChild(form);
        document.body.appendChild(dialog);
        let cancelPrompt;
        const promise = new Promise(resolve => {
            let finished = false;
            const finish = value => {
                if (finished) return;
                finished = true;
                input.value = '';
                dialog.remove();
                signal?.removeEventListener('abort', abort);
                completed?.();
                resolve(value);
            };
            const abort = () => finish(null);
            cancelPrompt = abort;
            form.addEventListener('submit', event => { event.preventDefault(); finish(input.value); });
            cancel.addEventListener('click', () => finish(null));
            dialog.addEventListener('cancel', event => { event.preventDefault(); finish(null); });
            signal?.addEventListener('abort', abort, {once: true});
            if (signal?.aborted) finish(null);
        });
        if (!signal?.aborted) {
            dialog.showModal();
            input.focus();
        }
        return {promise, cancel: cancelPrompt};
    }

    function confirmPassword() {
        // Preserve the existing non-intent/JSON behavior. A PG mode intent
        // never joins this promise or shares its password with another action.
        if (confirmation) return confirmation;
        intentPrompt?.cancel();
        legacyPrompt = passwordPrompt(null, () => { confirmation = null; legacyPrompt = null; });
        confirmation = legacyPrompt.promise;
        return confirmation;
    }

    const abortError = () => Object.assign(new Error('The original mode action ended.'), {name: 'AbortError'});
    const passwordValue = password => 'base64:' + btoa(Array.from(new TextEncoder().encode(password), byte => String.fromCharCode(byte)).join(''));

    async function fetchWithConfirmation(input, options, isCurrent, actionType) {
        const request = new Request(input, options);
        const current = () => !request.signal.aborted && (!isCurrent || isCurrent());
        if (isCurrent && !current()) throw abortError();
        const response = await originalFetch(request.clone());
        if (isCurrent && !current()) throw abortError();
        if (response.status !== 403 || request.method === 'GET' || request.headers.has(passwordHeader) || request.headers.has(intentHeader)) return response;
        const url = new URL(request.url);
        if (url.origin !== window.location.origin) return response;
        const body = await response.clone().json().catch(() => null);
        if (body?.code !== 'parent_authorization_required') return response;
        if (Object.hasOwn(body, 'parentIntent')) {
            // Only the exact original action type may consume its challenge.
            // A malformed PG challenge cannot downgrade to legacy confirmation.
            if (typeof body.parentIntent !== 'string') return response;
            const mode = actionType === undefined && request.method === 'PUT' && /\/api\/users\/[^/]+\/kids\/mode$/.test(url.pathname) && /^pmi_[0-9a-f]{64}$/.test(body.parentIntent);
            const listRemoval = actionType === 'listRemoval' && typeof isCurrent === 'function' && request.method === 'DELETE' && request.body === null && /\/api\/users\/[^/]+\/kids\/lists$/.test(url.pathname) && /^pli_[0-9a-f]{64}$/.test(body.parentIntent);
            const listAddition = actionType === 'listAddition' && typeof isCurrent === 'function' && request.method === 'POST' && request.body !== null && /\/api\/users\/[^/]+\/kids\/lists$/.test(url.pathname) && /^pla_[0-9a-f]{64}$/.test(body.parentIntent);
            const ratingPair = actionType === 'ratingPair' && typeof isCurrent === 'function' && request.method === 'PUT' && request.body !== null && /\/api\/users\/[^/]+\/kids\/ratings$/.test(url.pathname) && /^prp_[0-9a-f]{64}$/.test(body.parentIntent);
            const profileFlag = actionType === 'profileFlag' && typeof isCurrent === 'function' && request.method === 'PUT' && request.body !== null && /\/api\/profiles\/kids$/.test(url.pathname) && /^pkf_[0-9a-f]{64}$/.test(body.parentIntent);
            if (!mode && !listRemoval && !listAddition && !ratingPair && !profileFlag) return response;
            const generation = pageGeneration;
            if (!current()) throw abortError();
            legacyPrompt?.cancel();
            intentPrompt?.cancel();
            let prompt;
            prompt = passwordPrompt(request.signal, () => { if (intentPrompt === prompt) intentPrompt = null; });
            intentPrompt = request.signal.aborted ? null : prompt;
            // This is only a local maximum wait. Receipt delay never renews the
            // server's original thirty-second expiry or grants local authority.
            const timer = setTimeout(() => prompt.cancel(), 30000);
            let password;
            try { password = await prompt.promise; } finally { clearTimeout(timer); }
            if (!current() || generation !== pageGeneration) throw abortError();
            if (password === null) return response;
            const headers = new Headers(request.headers);
            headers.set(passwordHeader, passwordValue(password));
            headers.set(intentHeader, body.parentIntent);
            // Use the frozen Request, including its original body and signal.
            // A consumed/expired response is returned as-is, never challenged again.
            const retried = await originalFetch(new Request(request, {headers}));
            if (!current() || generation !== pageGeneration) throw abortError();
            if (profileFlag) profileFlagResponses.add(retried);
            return retried;
        }
        const password = await confirmPassword();
        if (isCurrent && !current()) throw abortError();
        if (password === null) return response;
        const headers = new Headers(request.headers);
        headers.set(passwordHeader, passwordValue(password));
        const retried = await originalFetch(new Request(request, { headers }));
        if (isCurrent && !current()) throw abortError();
        return retried;
    }
    window.fetch = (input, options) => fetchWithConfirmation(input, options);
    // The actual mode form supplies its original instance/profile/mode/action
    // predicate; this UI check is bookkeeping, never a server authorization proof.
    window.parentModeFetch = (input, options, isCurrent) => fetchWithConfirmation(input, options, isCurrent);
    window.parentListRemovalFetch = (input, options, isCurrent) => fetchWithConfirmation(input, options, isCurrent, 'listRemoval');
    window.parentListAdditionFetch = (input, options, isCurrent) => fetchWithConfirmation(input, options, isCurrent, 'listAddition');
    window.parentRatingFetch = (input, options, isCurrent) => fetchWithConfirmation(input, options, isCurrent, 'ratingPair');
    window.parentProfileFlagFetch = (input, options, isCurrent) => {
        if (typeof isCurrent !== 'function') return Promise.reject(new TypeError('Original profile action required.'));
        return fetchWithConfirmation(input, options, isCurrent, 'profileFlag');
    };

    // Changed/unavailable can follow a known COMMIT and failed local receipt
    // ACK. Only definite precommit refusals permit another explicit Save.
    window.profileFlagDefinitelyRefused = (status, value) =>
        (status === 403 && ['parent_authorization_required', 'forbidden', 'profile_locked'].includes(value?.code)) ||
        (status === 429 && value?.code === 'rate_limited') ||
        ([400, 413].includes(status) && value?.code === 'invalid_request');
    const exactObject = (value, keys) => value !== null && typeof value === 'object' && !Array.isArray(value) &&
        Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
    window.validateRecoveredProfile = (value, id) => {
        const fields = ['id', 'name', 'isKidsProfile', 'kidsMode', 'kidsMaxRating', 'kidsMaxMovieRating', 'kidsMaxTVRating',
            'kidsAllowedLists', 'allowLiveTakeover', 'allowRecordingInterruption'];
        return exactObject(value, ['profile', 'recovery']) && exactObject(value.profile, fields) &&
            exactObject(value.recovery, ['usersWriteBlocked', 'settingsPendingUncertainty']) &&
            value.profile.id === id && typeof value.profile.name === 'string' && typeof value.profile.isKidsProfile === 'boolean' &&
            ['kidsMode', 'kidsMaxRating', 'kidsMaxMovieRating', 'kidsMaxTVRating'].every(key => typeof value.profile[key] === 'string') &&
            Array.isArray(value.profile.kidsAllowedLists) && value.profile.kidsAllowedLists.every(item => typeof item === 'string') &&
            typeof value.profile.allowLiveTakeover === 'boolean' && typeof value.profile.allowRecordingInterruption === 'boolean' &&
            typeof value.recovery.usersWriteBlocked === 'boolean' && typeof value.recovery.settingsPendingUncertainty === 'boolean';
    };
    // Cookie flag replies are partial. In particular their default privacy and
    // absent accountId must never overwrite unrelated confirmed profile facts.
    window.profileFlagProjection = (value, id, desired, response) => {
        if (!value || value.id !== id || value.isKidsProfile !== desired ||
            typeof value.allowLiveTakeover !== 'boolean' || typeof value.allowRecordingInterruption !== 'boolean') {
            throw new TypeError('Invalid saved profile flag.');
        }
        const result = {isKidsProfile: desired};
        if (profileFlagResponses.has(response)) {
            result.allowLiveTakeover = value.allowLiveTakeover;
            result.allowRecordingInterruption = value.allowRecordingInterruption;
        } else if (desired) {
            // Existing JSON enable always clears both rights. Its serializer
            // does not populate these fields; JSON disable must preserve prior
            // cached facts until the existing known-success GET or explicit reload.
            result.allowLiveTakeover = false;
            result.allowRecordingInterruption = false;
        }
        for (const key of ['kidsMode', 'kidsMaxRating', 'kidsMaxMovieRating', 'kidsMaxTVRating']) {
            if (Object.hasOwn(value, key) && typeof value[key] !== 'string') throw new TypeError('Invalid saved profile flag.');
            result[key] = value[key] ?? '';
        }
        if (value.kidsAllowedLists !== undefined && value.kidsAllowedLists !== null &&
            (!Array.isArray(value.kidsAllowedLists) || !value.kidsAllowedLists.every(item => typeof item === 'string'))) {
            throw new TypeError('Invalid saved profile flag.');
        }
        result.kidsAllowedLists = value.kidsAllowedLists?.slice() ?? null;
        return result;
    };
    // This is an explicit read/adopt operation, not a parental mutation. It
    // never enters either password promise or retries any response.
    window.profileRecoveryFetch = async (input, options, isCurrent) => {
        const request = new Request(input, options);
        const url = new URL(request.url);
        const generation = pageGeneration;
        if (typeof isCurrent !== 'function' || request.method !== 'POST' || request.body !== null ||
            url.origin !== window.location.origin || url.username || url.password || url.search || url.hash ||
            !/\/api\/users\/[^/]+\/state\/reload$/.test(url.pathname) ||
            request.headers.has(passwordHeader) || request.headers.has(intentHeader)) {
            throw new TypeError('Invalid profile recovery request.');
        }
        const current = () => generation === pageGeneration && !request.signal.aborted && isCurrent();
        if (!current()) throw abortError();
        const response = await originalFetch(new Request(request, {
            credentials: 'same-origin', mode: 'same-origin', redirect: 'error', cache: 'no-store'
        }));
        if (!current()) throw abortError();
        return response;
    };

    // Creation locators are browser bookkeeping, never authentication or write
    // capabilities. Each tab/actor/exact management root has its own finite set.
    const creationHeader = 'X-Profile-Creation-Operation';
    const creationRef = value => typeof value === 'string' && value.length === 68 && /^pcr_[0-9a-f]{64}$/.test(value);
    const creationIntent = value => typeof value === 'string' && value.length === 68 && /^pcc_[0-9a-f]{64}$/.test(value);
    const creationClients = new WeakMap();
    const creationResponses = new WeakSet();
    const creationResponseRefs = new WeakMap();
    const encodedBytes = value => new TextEncoder().encode(value).length;
    const plainObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
    const creationID = value => typeof value === 'string' && value.length > 0 && value === value.trim() &&
        encodedBytes(value) <= 256 && !/[\u0000-\u001f]/.test(value);
    const creationStorageError = () => new Error('The local creation check could not be safely read or retained.');
    const creationCurrent = (action, generation) => typeof action?.isCurrent === 'function' &&
        action.signal && !action.signal.aborted && generation === pageGeneration && action.isCurrent();
    function requireCreationCurrent(action, generation) {
        if (!creationCurrent(action, generation)) throw abortError();
    }
    function creationNamespace(client) {
        const state = creationClients.get(client);
        if (!state) throw new TypeError('Original creation namespace required.');
        return state;
    }
    function readCreationContainer(state) {
        let raw;
        try { raw = window.sessionStorage.getItem(state.key); } catch (_) { throw creationStorageError(); }
        if (raw === null) return {version:1, actorAccountID:state.actorAccountID, basePath:state.basePath, entries:[]};
        if (typeof raw !== 'string' || encodedBytes(raw) > 8192) throw creationStorageError();
        let value;
        try { value = JSON.parse(raw); } catch (_) { throw creationStorageError(); }
        if (!exactObject(value, ['version','actorAccountID','basePath','entries']) || value.version !== 1 ||
            value.actorAccountID !== state.actorAccountID || value.basePath !== state.basePath ||
            !Array.isArray(value.entries) || value.entries.length > 8) throw creationStorageError();
        const refs = new Set();
        for (const entry of value.entries) {
            if (!exactObject(entry, ['ref','ownerID','createdAt']) || typeof entry.ref !== 'string' ||
                !creationRef(entry.ref) || !creationID(entry.ownerID) || typeof entry.createdAt !== 'string' ||
                !Number.isFinite(Date.parse(entry.createdAt)) || new Date(entry.createdAt).toISOString() !== entry.createdAt || refs.has(entry.ref)) {
                throw creationStorageError();
            }
            refs.add(entry.ref);
        }
        return value;
    }
    function writeCreationContainer(state, value) {
        const raw = JSON.stringify(value);
        if (encodedBytes(raw) > 8192 || value.entries.length > 8) throw creationStorageError();
        try {
            window.sessionStorage.setItem(state.key, raw);
            if (window.sessionStorage.getItem(state.key) !== raw) throw creationStorageError();
        } catch (_) { throw creationStorageError(); }
        // Parse the exact persisted container too; silent coercion/corruption
        // must not permit the subsequent password-bearing retry.
        return readCreationContainer(state);
    }
    function removeCreationRef(state, ref) {
        if (typeof ref !== 'string' || !creationRef(ref)) throw new TypeError('Invalid creation reference.');
        const value = readCreationContainer(state);
        if (!value.entries.some(entry => entry.ref === ref)) return false;
        value.entries = value.entries.filter(entry => entry.ref !== ref);
        writeCreationContainer(state, value);
        return true;
    }
    window.createProfileCreationClient = ({actorAccountID, basePath} = {}) => {
        if (!creationID(actorAccountID) || typeof basePath !== 'string' || !basePath.startsWith('/') || basePath.startsWith('//') ||
            !/\/(admin|account)$/.test(basePath)) throw new TypeError('Invalid creation namespace.');
        const url = new URL(basePath, window.location.origin);
        if (url.origin !== window.location.origin || url.pathname !== basePath || url.username || url.password || url.search || url.hash ||
            !/\/(admin|account)$/.test(url.pathname)) throw new TypeError('Invalid creation namespace.');
        const state = {actorAccountID, basePath,
            createURL:new URL(basePath + '/api/profiles', window.location.origin).href,
            key:'mediastorm.profileCreation.v1:' + encodeURIComponent(actorAccountID) + ':' + encodeURIComponent(basePath)};
        const client = Object.freeze({
            list: () => readCreationContainer(state).entries.map(entry => ({ref:entry.ref,ownerID:entry.ownerID,createdAt:entry.createdAt})),
            resolve: ref => removeCreationRef(state, ref),
            forget: ref => removeCreationRef(state, ref)
        });
        creationClients.set(client, state);
        return client;
    };
    function retainCreationRef(state, ref, ownerID) {
        const value = readCreationContainer(state);
        if (value.entries.some(entry => entry.ref === ref) || value.entries.length >= 8) throw creationStorageError();
        value.entries.push({ref, ownerID, createdAt:new Date().toISOString()});
        writeCreationContainer(state, value);
    }
    function checkRetainedCreationRef(state, ref, ownerID) {
        const entries = readCreationContainer(state).entries;
        if (!entries.some(entry => entry.ref === ref && entry.ownerID === ownerID)) throw creationStorageError();
    }
    function creationRequest(input, options, action) {
        if (typeof action?.isCurrent !== 'function' || !action.signal || typeof action.signal.aborted !== 'boolean') {
            throw new TypeError('Original creation action required.');
        }
        const target = typeof input === 'string' || input instanceof URL ? new URL(input, window.location.origin).href : input;
        return new Request(target, {...options, signal:action.signal});
    }
    async function creationPasswordPrompt(action, generation) {
        if (!creationCurrent(action, generation)) return null;
        legacyPrompt?.cancel();
        intentPrompt?.cancel();
        let prompt;
        prompt = passwordPrompt(action.signal, () => { if (intentPrompt === prompt) intentPrompt = null; },
            'Confirm the account password to create this child profile. Optional icon or PIN changes are not applied automatically; set them later in Accounts.');
        intentPrompt = action.signal.aborted ? null : prompt;
        // A local wait limit cannot extend the captured server authority.
        const timer = setTimeout(() => prompt.cancel(), 30000);
        try { return await prompt.promise; } finally { clearTimeout(timer); }
    }
    window.parentProfileCreationFetch = async (client, input, options, action) => {
        const state = creationNamespace(client);
        if (!creationID(action?.ownerID)) throw new TypeError('Original selected account required.');
        const request = creationRequest(input, options, action);
        const url = new URL(request.url), generation = pageGeneration;
        if (request.method !== 'POST' || request.body === null || request.url !== state.createURL ||
            url.origin !== window.location.origin || url.username || url.password || url.search || url.hash ||
            request.headers.has(passwordHeader) || request.headers.has(intentHeader) || request.headers.has(creationHeader)) {
            throw new TypeError('Invalid profile creation request.');
        }
        requireCreationCurrent(action, generation);
        const response = await originalFetch(new Request(request.clone(), {
            credentials:'same-origin', mode:'same-origin', redirect:'error', cache:'no-store'
        }));
        requireCreationCurrent(action, generation);
        if (response.status !== 403) return response;
        const body = await response.clone().json().catch(() => null);
        requireCreationCurrent(action, generation);
        if (body?.code !== 'parent_authorization_required') return response;
        if (Object.hasOwn(body, 'parentIntent') || Object.hasOwn(body, 'creationOperation')) {
            if (typeof body.parentIntent !== 'string' || !creationIntent(body.parentIntent) ||
                typeof body.creationOperation !== 'string' || !creationRef(body.creationOperation) || typeof body.error !== 'string') {
                return response;
            }
            const ref = body.creationOperation;
            // No write attempt happens before this synchronous verified retention.
            retainCreationRef(state, ref, action.ownerID);
            creationResponseRefs.set(response, ref);
            action.onLocatorAcknowledged?.(ref);
            if (!creationCurrent(action, generation)) return response;
            let password = await creationPasswordPrompt(action, generation);
            if (password === null || !creationCurrent(action, generation)) return response;
            const headers = new Headers(request.headers);
            headers.set(passwordHeader, passwordValue(password));
            headers.set(intentHeader, body.parentIntent);
            headers.set(creationHeader, ref);
            password = null;
            // Re-read immediately before dispatch. No async gap follows this
            // point until the conservative possibly-sent callback and fetch.
            checkRetainedCreationRef(state, ref, action.ownerID);
            if (!creationCurrent(action, generation)) return response;
            action.onRetryDispatched?.(ref);
            if (!creationCurrent(action, generation)) return response;
            const retried = await originalFetch(new Request(request, {
                headers, credentials:'same-origin', mode:'same-origin', redirect:'error', cache:'no-store'
            }));
            requireCreationCurrent(action, generation);
            creationResponses.add(retried);
            creationResponseRefs.set(retried, ref);
            if (retried.ok) {
                const saved = await retried.clone().json();
                requireCreationCurrent(action, generation);
                if (retried.status !== 200 || saved?.creationOperation !== ref) throw new TypeError('Creation result could not be confirmed; retain its check.');
                window.decodeProfileCreationProfile(saved);
            }
            return retried;
        }
        // Preserve existing JSON/adult no-intent confirmation without sending a
        // second initial request or consuming another action's PG challenge.
        const cancelLegacy = () => legacyPrompt?.cancel();
        action.signal.addEventListener('abort', cancelLegacy, {once:true});
        let password;
        try { password = await confirmPassword(); }
        finally { action.signal.removeEventListener('abort', cancelLegacy); }
        requireCreationCurrent(action, generation);
        if (password === null) return response;
        const headers = new Headers(request.headers);
        headers.set(passwordHeader, passwordValue(password));
        password = null;
        const retried = await originalFetch(new Request(request, {
            headers, credentials:'same-origin', mode:'same-origin', redirect:'error', cache:'no-store'
        }));
        requireCreationCurrent(action, generation);
        return retried;
    };
    window.profileCreationResponseIsQualified = response => creationResponses.has(response);
    window.profileCreationResponseOperation = response => creationResponseRefs.get(response) || null;
    window.profileCreationResultReadFetch = async (client, ref, action) => {
        const state = creationNamespace(client);
        if (typeof ref !== 'string' || !creationRef(ref)) throw new TypeError('Invalid creation reference.');
        const request = creationRequest(new URL(state.basePath + '/api/profiles/creation-operations/' + encodeURIComponent(ref) + '/read', window.location.origin).href,
            {method:'POST',credentials:'same-origin',mode:'same-origin',redirect:'error',cache:'no-store'},action);
        const generation = pageGeneration;
        requireCreationCurrent(action, generation);
        const response = await originalFetch(request);
        requireCreationCurrent(action, generation);
        return response;
    };
    const creationProfileBooleans = ['hasPin','hasIcon','isKidsProfile','allowShareLinks','allowLiveTakeover','allowRecordingInterruption'];
    const creationProfileOptional = ['accountId','color','iconUrl','kidsMode','kidsMaxRating','kidsMaxMovieRating','kidsMaxTVRating',
        'traktAccountId','mdblistAccountId','simklAccountId','scrobAccountId'];
    function creationDate(value) {
        if (typeof value !== 'string') return false;
        // Go's RFC3339 JSON dates can contain fractional nanoseconds and an
        // explicit offset. Validate the original calendar before Date.parse,
        // which otherwise accepts abbreviated dates and normalizes bad days.
        const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
        if (!match || match[0].length !== value.length) return false;
        const year = Number(match[1]), month = Number(match[2]), day = Number(match[3]);
        const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
        const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1] &&
            Number(match[4]) <= 23 && Number(match[5]) <= 59 && Number(match[6]) <= 59 &&
            (!match[7] || Number(match[8]) <= 23 && Number(match[9]) <= 59) && Number.isFinite(Date.parse(value));
    }
    function decodeCreationProfile(value, allowOperation) {
        if (!plainObject(value) || !creationID(value.id) || typeof value.name !== 'string' ||
            typeof value.activityPrivacy !== 'string' || !creationProfileBooleans.every(key => typeof value[key] === 'boolean') ||
            !['createdAt','updatedAt'].every(key => creationDate(value[key]))) {
            throw new TypeError('Invalid saved profile.');
        }
        const unsafe = ['password','passwordHash','password_hash','pin','pinHash','pin_hash','session','sessionToken','session_token',
            'token','reference_hash','marker_id','result_version','operation_id','revision'];
        if (unsafe.some(key => Object.hasOwn(value,key)) ||
            Object.hasOwn(value,'creationOperation') && (!allowOperation || typeof value.creationOperation !== 'string' || !creationRef(value.creationOperation))) {
            throw new TypeError('Unsafe saved profile fields.');
        }
        const result = {id:value.id,name:value.name,activityPrivacy:value.activityPrivacy,createdAt:value.createdAt,updatedAt:value.updatedAt};
        for (const key of creationProfileBooleans) result[key] = value[key];
        for (const key of creationProfileOptional) {
            if (!Object.hasOwn(value,key)) continue;
            if (typeof value[key] !== 'string') throw new TypeError('Invalid saved profile field.');
            result[key] = value[key];
        }
        if (Object.hasOwn(value,'kidsAllowedLists')) {
            if (!Array.isArray(value.kidsAllowedLists) || !value.kidsAllowedLists.every(item => typeof item === 'string')) throw new TypeError('Invalid saved profile lists.');
            result.kidsAllowedLists = value.kidsAllowedLists.slice();
        }
        return result;
    }
    window.decodeProfileCreationProfile = value => decodeCreationProfile(value,true);
    window.decodeProfileCreationResult = (status,value) => {
        if (!plainObject(value) || typeof value.outcome !== 'string' ||
            typeof value.usersWriteBlocked !== 'boolean' || typeof value.settingsPendingUncertainty !== 'boolean') {
            throw new TypeError('Invalid creation result.');
        }
        if (status === 404 ? value.outcome !== 'unqualified_or_missing' :
            status !== 200 || !['pending','created','not_created','changed_or_replaced'].includes(value.outcome)) {
            throw new TypeError('Unavailable creation result.');
        }
        if (value.outcome !== 'created' && Object.hasOwn(value,'profile')) throw new TypeError('Unexpected creation result profile.');
        const result = {outcome:value.outcome,usersWriteBlocked:value.usersWriteBlocked,settingsPendingUncertainty:value.settingsPendingUncertainty};
        if (Object.hasOwn(value,'profile')) result.profile = decodeCreationProfile(value.profile,false);
        return result;
    };

    window.addEventListener?.('pagehide', () => { pageGeneration++; intentPrompt?.cancel(); });
})();
