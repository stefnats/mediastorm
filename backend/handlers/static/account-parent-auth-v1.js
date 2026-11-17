// Retry parental-control mutations only after fresh confirmation by the owner.
(() => {
    'use strict';
    const originalFetch = window.fetch.bind(window);
    const passwordHeader = 'X-Account-Password';
    const intentHeader = 'X-Parent-Intent';
    let confirmation = null;
    let legacyPrompt = null;
    let intentPrompt = null;
    let pageGeneration = 0;

    function passwordPrompt(signal, completed) {
        const dialog = document.createElement('dialog');
        dialog.setAttribute('aria-label', 'Confirm account password');
        dialog.style.cssText = 'max-width:26rem;width:calc(100% - 3rem);padding:1.5rem;border:1px solid #444;border-radius:12px;background:#18181b;color:#fafafa;';
        const form = document.createElement('form');
        const title = document.createElement('h2');
        title.textContent = 'Parent confirmation';
        const explanation = document.createElement('p');
        explanation.textContent = 'Enter the account password to change parental controls or manage profiles.';
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
            if (!mode && !listRemoval && !listAddition && !ratingPair) return response;
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
    window.addEventListener?.('pagehide', () => { pageGeneration++; intentPrompt?.cancel(); });
})();
