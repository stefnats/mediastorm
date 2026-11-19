/* Explicit joined-copy actions. A stored container observation is not a completeness guarantee. */
(function (global) {
    'use strict';
    const unknownReasons = new Set(['not_prepared', 'configuration_changed', 'source_changed', 'expired']);
    const maxTTL = 24 * 60 * 60 * 1000;
    function details(row) {
        const info = global.RecordingExportUI.details(row);
        if (!info || !info.exportable || info.count < 2 || info.indices.some((index, offset) => index !== offset + 1)) return false;
        const gaps = row.capture.gaps;
        return Array.isArray(gaps) && gaps.length <= 256 && gaps.every(gap => gap && Number.isFinite(Date.parse(gap.startedAt)) && Number.isFinite(Date.parse(gap.endedAt)) && Date.parse(gap.endedAt) >= Date.parse(gap.startedAt));
    }
    function utc(value) {
        if (typeof value !== 'string') return NaN;
        const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
        if (!match) return NaN;
        const at = new Date(value), parsed = at.getTime();
        if (!Number.isFinite(parsed) || at.getUTCFullYear() !== Number(match[1]) || at.getUTCMonth() + 1 !== Number(match[2]) || at.getUTCDate() !== Number(match[3]) || at.getUTCHours() !== Number(match[4]) || at.getUTCMinutes() !== Number(match[5]) || at.getUTCSeconds() !== Number(match[6])) return NaN;
        return parsed;
    }
    function create(options) {
        const dialog = options.dialog, pinInput = dialog.querySelector('[data-derived-pin]'), pinStatus = dialog.querySelector('[data-derived-pin-status]');
        let rows = new Map(), boundProfile = '', boundGeneration = -1, action = 0;
        let active = null, activeIntent = null, pending = null, ready = null, unlock = null, lastClock = null, expiryTimer = null, receiver = null, sequence = 0;
        function current(intent) {
            return intent && intent.action === action && intent.profile === boundProfile && intent.profile === options.profile() && intent.generation === boundGeneration && intent.generation === options.generation() && rows.get(intent.id) === intent.row && details(intent.row) && intent.row.userId === intent.profile;
        }
        function endpoint(base, suffix) {
            const value = new URL(base + suffix, location.href);
            if (value.origin !== location.origin || value.username || value.password || value.search || value.hash) throw new Error('Invalid recording endpoint');
            return value.href;
        }
        function clockNow(clock) {
            const elapsed = performance.now() - clock.received;
            if (!Number.isFinite(elapsed) || elapsed < 0) throw new Error('Invalid monotonic clock');
            return clock.upper + elapsed;
        }
        function responseClock(response, started) {
            const raw = response.headers.get('Date'), at = raw && Date.parse(raw), received = performance.now(), elapsed = received - started;
            if (new URL(response.url).origin !== location.origin || !raw || !/^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(raw) || !Number.isFinite(at) || new Date(at).toUTCString() !== raw || !Number.isFinite(elapsed) || elapsed < 0) throw new Error('Invalid server clock');
            const clock = { lower: at, upper: at + 1000 + elapsed, received };
            if (lastClock && clock.upper < lastClock.lower + (received - lastClock.received)) throw new Error('Server clock moved backwards');
            lastClock = clock; return clock;
        }
        async function smallJSON(response) {
            if (!response.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) throw new Error('Invalid response type');
            const reader = response.body.getReader(); let size = 0, chunks = [];
            try {
                while (true) {
                    const item = await reader.read(); if (item.done) break;
                    size += item.value.byteLength;
                    if (size > 64 * 1024) { await reader.cancel(); throw new Error('Oversized response'); }
                    chunks.push(item.value);
                }
            } finally { reader.releaseLock(); }
            const bytes = new Uint8Array(size); let offset = 0;
            chunks.forEach(chunk => { bytes.set(chunk, offset); offset += chunk.byteLength; });
            return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
        }
        function clearReady() {
            ready = null; options.openButton.hidden = true;
            if (expiryTimer !== null) clearTimeout(expiryTimer);
            expiryTimer = null;
        }
        function message(text) { options.status.textContent = text; }
        function closeReceiver() { if (receiver) receiver.remove(); receiver = null; options.closeButton.hidden = true; }
        function clearDialog() { pending = null; pinInput.value = ''; pinStatus.textContent = ''; if (dialog.open) dialog.close(); }
        function retire() {
            action++; if (active) active.abort(); active = null; activeIntent = null;
            clearDialog(); clearReady(); closeReceiver();
        }
        function askPIN(intent) {
            if (!current(intent)) return;
            clearDialog(); pending = intent; message('Unlock the original recording profile to continue this joined-copy action.');
            dialog.showModal(); pinInput.focus();
        }
        function validate(data, intent, clock) {
            if (!data || typeof data !== 'object' || Array.isArray(data) || data.recordingId !== intent.id || data.sourceStatus !== intent.row.status || typeof data.sourceIncomplete !== 'boolean' || typeof data.gapsOmitted !== 'boolean' || !Array.isArray(data.partOffsets)) throw new Error('Invalid joined-copy response');
            if (data.gapsOmitted && !data.sourceIncomplete || intent.row.capture.incomplete && !data.sourceIncomplete || data.state === 'ready' && intent.row.capture.gaps.length && (!data.gapsOmitted || !data.sourceIncomplete)) throw new Error('Incomplete provenance changed');
            const common = ['recordingId', 'state', 'sourceIncomplete', 'gapsOmitted', 'sourceStatus', 'partOffsets'];
            const expected = data.state === 'unknown' ? common.concat('reason') : common.concat('id', 'durationMs', 'bytes', 'observedAt', 'expiresAt');
            const keys = Object.keys(data);
            if (keys.length !== expected.length || keys.some(key => !expected.includes(key))) throw new Error('Unexpected joined-copy fields');
            if (data.state === 'unknown') {
                if (!unknownReasons.has(data.reason) || data.partOffsets.length !== 0) throw new Error('Invalid unknown joined copy');
                return data;
            }
            if (data.state !== 'ready' || typeof data.id !== 'string' || !/^[a-f0-9]{32}$/.test(data.id) || !Number.isSafeInteger(data.durationMs) || data.durationMs <= 0 || !Number.isSafeInteger(data.bytes) || data.bytes <= 0 || data.partOffsets.length !== intent.row.capture.parts.length) throw new Error('Invalid prepared joined copy');
            let end = 0;
            data.partOffsets.forEach((part, index) => {
                if (!part || Object.keys(part).length !== 3 || Object.keys(part).some(key => !['partIndex', 'startMs', 'durationMs'].includes(key)) || part.partIndex !== index + 1 || !Number.isSafeInteger(part.startMs) || part.startMs !== end || !Number.isSafeInteger(part.durationMs) || part.durationMs <= 0) throw new Error('Invalid joined-copy offsets');
                end += part.durationMs; if (!Number.isSafeInteger(end)) throw new Error('Oversized joined-copy timeline');
            });
            if (end !== data.durationMs) throw new Error('Joined-copy duration disagrees with recorded sections');
            const observed = utc(data.observedAt), expires = utc(data.expiresAt);
            if (!Number.isFinite(observed) || !Number.isFinite(expires) || observed > clockNow(clock) || expires <= observed || expires - observed > maxTTL) throw new Error('Invalid joined-copy lifetime');
            return data;
        }
        function show(intent, data, clock) {
            if (!current(intent)) return;
            clearReady();
            if (data.state === 'unknown') { message('No current joined copy is stored (' + data.reason.replaceAll('_', ' ') + '). Prepare explicitly if needed. Original parts remain unchanged.'); return; }
            const remaining = utc(data.expiresAt) - clockNow(clock);
            if (remaining <= 0) { message('The prepared joined copy expired. Read status or prepare explicitly again.'); return; }
            ready = { intent, data, clock }; options.openButton.hidden = false;
            const incomplete = data.sourceIncomplete ? ' Source recording is incomplete.' : '';
            const gaps = data.gapsOmitted ? ' Known closed gaps were omitted; missing content was not restored.' : ' No missing content has been inferred or restored.';
            message('Joined copy ready for explicit opening. Container duration: approximately ' + (data.durationMs / 1000).toFixed(1) + ' seconds.' + incomplete + gaps + ' This is not a complete or seamless programme guarantee. Original parts remain available.');
            expiryTimer = setTimeout(() => { if (current(intent)) { clearReady(); message('The prepared joined copy expired. Read status or prepare explicitly again.'); } }, Math.min(remaining, maxTTL));
        }
        function open(intent, token) {
            let unexpired = false;
            try { unexpired = ready && utc(ready.data.expiresAt) > clockNow(ready.clock); } catch (_) {}
            if (!current(intent) || !ready || ready.intent.row !== intent.row || ready.intent.action !== intent.action || !unexpired) { clearReady(); return; }
            closeReceiver();
            const frame = document.createElement('iframe'); frame.name = 'recording-derived-' + (++sequence);
            frame.title = 'Joined-copy download response'; frame.hidden = true;
            frame.referrerPolicy = 'no-referrer'; frame.setAttribute('sandbox', 'allow-same-origin allow-downloads');
            frame.addEventListener('load', function () {
                if (!current(intent) || receiver !== frame) return;
                let text = ''; try { text = frame.contentDocument?.body?.textContent || ''; } catch (_) { message('Joined-copy access unavailable. Reload recordings.'); return; }
                if (!text.trim()) return;
                let data = {}; try { if (text.length <= 8192) data = JSON.parse(text); } catch (_) { return; }
                if (data.code === 'profile_locked') { unlock = null; closeReceiver(); askPIN(intent); return; }
                if (data.code || data.error) { closeReceiver(); clearReady(); message('Joined-copy access unavailable. Reload recordings and read status explicitly.'); }
            });
            options.receiverHost.appendChild(frame); receiver = frame; options.closeButton.hidden = false;
            const form = document.createElement('form'); form.hidden = true; form.method = 'POST'; form.enctype = 'application/x-www-form-urlencoded';
            form.action = endpoint(options.apiBase(), '/' + encodeURIComponent(intent.id) + '/derived/' + ready.data.id + '/stream'); form.target = frame.name;
            for (const [name, value] of [['profileId', intent.profile], ['profileUnlock', token || '']]) {
                const input = document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = value; form.appendChild(input);
            }
            document.body.appendChild(form);
            if (!current(intent)) { form.remove(); closeReceiver(); return; }
            message('Joined-copy download requested. Save the file to your device. Original capture may remain incomplete; omitted gaps are not recovered.');
            form.submit(); form.remove();
        }
        async function request(intent, token) {
            if (!current(intent)) return;
            if (intent.kind === 'open') { open(intent, token); return; }
            const controller = new AbortController(); active = controller; activeIntent = intent;
            const timeout = setTimeout(() => controller.abort(), 310_000);
            message(intent.kind === 'join' ? 'Preparing this joined copy. Cancel to stop the request. Original parts remain unchanged.' : 'Reading stored joined-copy status. No inspection or preparation is started.');
            try {
                const started = performance.now();
                const response = await fetch(endpoint(options.apiBase(), '/' + encodeURIComponent(intent.id) + '/derived/' + intent.kind), {
                    method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: controller.signal,
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ profileId: intent.profile, profileUnlock: token || '' })
                });
                if (!current(intent) || active !== controller) return;
                if (!response.ok) {
                    let denied = {}; if (response.status === 403) { try { denied = await smallJSON(response); } catch (_) {} }
                    if (!current(intent) || active !== controller) return;
                    if (denied.code === 'profile_locked') { unlock = null; active = null; activeIntent = null; askPIN(intent); return; }
                    message(response.status === 429 ? 'Joined-copy preparation is busy. Retry explicitly later.' : response.status === 409 ? 'This recording cannot currently be joined. Original parts remain available; reload or read status explicitly.' : 'Joined-copy access unavailable. Sign in again or reload recordings.');
                    return;
                }
                const clock = responseClock(response, started), data = validate(await smallJSON(response), intent, clock);
                if (current(intent) && active === controller) show(intent, data, clock);
            } catch (_) { if (current(intent) && active === controller) message('Joined-copy request did not finish. No ready result was adopted. Retry explicitly if needed.'); }
            finally { clearTimeout(timeout); if (active === controller) { active = null; activeIntent = null; } }
        }
        function tokenFor(intent) {
            try { if (unlock && unlock.profile === intent.profile && unlock.expires > clockNow(unlock.clock)) return unlock.token; } catch (_) {}
            unlock = null; return '';
        }
        options.container.addEventListener('click', function (event) {
            if (event.target.closest('[data-recording-derived-cancel]')) { retire(); message('Joined-copy request cancelled. No new preparation or stream was requested.'); return; }
            const button = event.target.closest('[data-recording-derived]');
            if (!button || !options.container.contains(button)) return;
            const id = button.getAttribute('data-recording-derived'), kind = button.getAttribute('data-derived-operation');
            if (!['join', 'read'].includes(kind)) return;
            retire(); const intent = { id, row: rows.get(id), profile: boundProfile, generation: boundGeneration, action, kind };
            if (current(intent)) request(intent, tokenFor(intent));
        });
        options.openButton.addEventListener('click', function () {
            if (!ready || !current(ready.intent)) { clearReady(); return; }
            const intent = Object.assign({}, ready.intent, { kind: 'open' });
            request(intent, tokenFor(intent));
        });
        options.closeButton.addEventListener('click', closeReceiver);
        function cancelPIN() { retire(); message('PIN check cancelled. No new joined copy or stream was requested.'); }
        dialog.querySelector('[data-derived-pin-cancel]').addEventListener('click', cancelPIN);
        dialog.addEventListener('cancel', function (event) { event.preventDefault(); cancelPIN(); });
        dialog.querySelector('form').addEventListener('submit', async function (event) {
            event.preventDefault(); const intent = pending;
            if (!current(intent) || active) return;
            const pin = pinInput.value; pinInput.value = '';
            if (!pin || pin.length > 256) { pinStatus.textContent = 'Enter the profile PIN.'; return; }
            const controller = new AbortController(); active = controller; activeIntent = intent; pinStatus.textContent = 'Checking PIN...';
            try {
                const started = performance.now();
                const response = await fetch(endpoint(options.pinBase(), '/' + encodeURIComponent(intent.profile) + '/pin/verify'), {
                    method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: controller.signal,
                    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pin })
                });
                const data = await smallJSON(response);
                if (!current(intent) || active !== controller) return;
                if (!response.ok || data.valid !== true) { pinStatus.textContent = data.code === 'pin_invalid' || data.code === 'pin_required' ? 'Incorrect PIN. Try again.' : 'PIN access unavailable. Sign in again or reload.'; return; }
                const clock = responseClock(response, started), expires = utc(data.expiresAt), token = data.unlockToken;
                if (typeof token !== 'string' || token.includes(',') || !/^[\x21-\x7e]{1,2048}$/.test(token) || !Number.isFinite(expires) || expires <= clockNow(clock)) throw new Error('Invalid unlock');
                unlock = { profile: intent.profile, token, expires, clock };
                active = null; activeIntent = null; clearDialog(); if (current(intent)) request(intent, token);
            } catch (_) { if (current(intent) && active === controller) pinStatus.textContent = 'PIN check unavailable. Retry explicitly.'; }
            finally { if (active === controller) { active = null; activeIntent = null; } }
        });
        return {
            bind(items, profile, generation) {
                rows = new Map(items.filter(row => row && typeof row.id === 'string').map(row => [row.id, row])); boundProfile = profile; boundGeneration = generation;
                if (activeIntent && !current(activeIntent) || pending && !current(pending) || ready && !current(ready.intent)) {
                    retire(); message('Recording list changed. Choose the current row explicitly; no previous result was adopted.');
                }
            },
            invalidate() { retire(); rows.clear(); unlock = null; lastClock = null; boundProfile = ''; boundGeneration = -1; message(''); }
        };
    }
    global.RecordingDerivedUI = Object.freeze({ details, create });
})(window);
