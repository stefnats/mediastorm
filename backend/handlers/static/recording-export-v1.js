/* Browser-only transport for original recording parts. No archive buffering. */
(function (global) {
    'use strict';
    const terminal = new Set(['completed', 'failed', 'cancelled']);
    const closed = new Set(['schedule', 'source_ended', 'capture_failed', 'shutdown', 'cancelled']);
    function details(row) {
        if (!row || typeof row.id !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(row.id) || typeof row.userId !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(row.userId)) return null;
        const capture = row.capture;
        if (!capture || capture.version !== 1 || typeof capture.incomplete !== 'boolean' || !Array.isArray(capture.parts) || !capture.parts.length || capture.parts.length > 256) return null;
        const indices = new Set();
        for (const part of capture.parts) {
            if (!Number.isInteger(part.index) || part.index < 1 || part.index > 256 || indices.has(part.index)) return null;
            indices.add(part.index);
        }
        const allClosed = capture.parts.every(part => Number.isFinite(Date.parse(part.startedAt)) && Number.isFinite(Date.parse(part.endedAt)) && Date.parse(part.endedAt) >= Date.parse(part.startedAt) && closed.has(part.endReason));
        return { count: capture.parts.length, indices: [...indices].sort((a, b) => a - b), incomplete: capture.incomplete, exportable: terminal.has(row.status) && allClosed };
    }
    function create(options) {
        const status = options.status;
        const dialog = options.dialog;
        const pinInput = dialog.querySelector('[data-export-pin]');
        const pinStatus = dialog.querySelector('[data-export-pin-status]');
        let rows = new Map(), boundProfile = '', boundGeneration = -1;
        let unlock = null, pending = null, controller = null;
        let frames = [], sequence = 0, actionGeneration = 0, expiryTimer = null;
        let selections = new Map(), lastClock = null;
        function current(intent) {
            return intent && intent.action === actionGeneration && intent.profile === options.profile() && intent.generation === options.generation() && intent.profile === boundProfile && intent.generation === boundGeneration && rows.has(intent.id) && (intent.kind === 'export' || selectedPart(intent.id) === intent.part);
        }
        function clockNow(clock) {
            const elapsed = performance.now() - clock.received;
            if (!Number.isFinite(elapsed) || elapsed < 0) throw new Error('Invalid monotonic clock');
            return clock.upper + elapsed;
        }
        function responseClock(response, started) {
            const url = new URL(response.url);
            const raw = response.headers.get('Date');
            const value = raw && Date.parse(raw);
            if (url.origin !== location.origin || !raw || !/^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(raw) || !Number.isFinite(value) || new Date(value).toUTCString() !== raw) throw new Error('Missing or invalid server clock');
            const received = performance.now(), roundTrip = received - started;
            if (!Number.isFinite(started) || !Number.isFinite(roundTrip) || roundTrip < 0) throw new Error('Invalid request clock');
            // HTTP-Date has one-second precision. Expiry uses its upper bound;
            // the measured original request RTT also bounds delayed delivery.
            // No DTO timestamp or client wall-clock can extend a credential.
            const clock = { lower: value, upper: value + 1000 + roundTrip, received };
            if (lastClock && clock.upper < lastClock.lower + (received - lastClock.received)) throw new Error('Server clock moved backwards');
            lastClock = clock; return clock;
        }
        function endpoint(base, suffix) {
            const url = new URL(base + suffix, location.href);
            if (url.origin !== location.origin || url.username || url.password || url.search || url.hash) throw new Error('Invalid recording endpoint');
            return url.href;
        }
        function message(text) {
            if (expiryTimer !== null) clearTimeout(expiryTimer);
            expiryTimer = null; status.textContent = text;
        }
        function selectedPart(id) {
            const info = details(rows.get(id));
            const chosen = selections.get(id);
            return info && info.indices.includes(chosen) ? chosen : info && info.indices[0];
        }
        function clearDialog() {
            if (controller) controller.abort();
            controller = null; pending = null; pinInput.value = ''; pinStatus.textContent = '';
            if (dialog.open) dialog.close();
        }
        function askPIN(intent) {
            if (!current(intent)) return;
            clearDialog(); pending = intent;
            const exporting = intent.kind === 'export';
            dialog.querySelector('[data-recording-pin-purpose]').textContent = exporting ? "Enter this profile's PIN to download its original parts. The parts remain separate files." : "Enter this profile's PIN for the selected part's estimated length. No parts are merged.";
            dialog.querySelector('[data-recording-pin-submit]').textContent = exporting ? 'Unlock and download' : intent.kind === 'read' ? 'Unlock and view estimate' : 'Unlock and inspect';
            message(exporting ? "Unlock this recording profile to request its parts ZIP." : 'Unlock this recording profile for the selected part estimate.');
            dialog.showModal(); pinInput.focus();
        }
        function submit(intent, token) {
            if (!current(intent)) return;
            if (intent.kind !== 'export') { requestTiming(intent, token); return; }
            const frame = document.createElement('iframe');
            frame.hidden = true; frame.name = 'recording-export-' + (++sequence);
            frame.setAttribute('sandbox', 'allow-same-origin allow-downloads');
            frame.referrerPolicy = 'no-referrer';
            frame.addEventListener('load', function () {
                if (!current(intent)) return;
                let text;
                try { text = frame.contentDocument && frame.contentDocument.body && frame.contentDocument.body.textContent; } catch (_) { message('Download unavailable. Sign in again or reload recordings.'); return; }
                if (!text || !text.trim()) return;
                let data = {};
                try { if (text.length < 8192) data = JSON.parse(text); } catch (_) {}
                if (data.code === 'profile_locked') { unlock = null; askPIN(intent); return; }
                message(data.code === 'kids_live_tv_unavailable' ? 'Recordings are unavailable for kids profiles.' : 'Download unavailable. Reload recordings and try again; existing files have not been changed.');
            });
            document.body.appendChild(frame); frames.push(frame);
            // Retain at most two receiver contexts. Browser downloads already
            // accepted are managed by the browser, not a JavaScript completion flag.
            while (frames.length > 2) frames.shift().remove();
            const form = document.createElement('form');
            form.hidden = true; form.method = 'POST'; form.enctype = 'application/x-www-form-urlencoded';
            form.action = endpoint(options.apiBase(), '/' + encodeURIComponent(intent.id) + '/export'); form.target = frame.name;
            for (const [name, value] of [['profileId', intent.profile], ['profileUnlock', token || '']]) {
                const input = document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = value; form.appendChild(input);
            }
            document.body.appendChild(form);
            if (!current(intent)) { form.remove(); frame.remove(); return; }
            message('Download requested. Check your browser downloads for completion or cancel there. Parts are not merged.');
            form.submit(); form.remove();
        }
        const unknownReasons = new Set(['no_observation', 'observation_expired', 'ledger_changed', 'probe_unavailable', 'unsupported_container']);
        function validateTiming(data, intent, clock) {
            if (!data || typeof data !== 'object' || Array.isArray(data) || data.recordingId !== intent.id || data.partIndex !== intent.part) throw new Error('Invalid timing response');
            const keys = Object.keys(data);
            if (data.state === 'unknown') {
                if (keys.some(key => !['recordingId', 'partIndex', 'state', 'reason'].includes(key)) || !unknownReasons.has(data.reason)) throw new Error('Invalid unknown estimate');
                return data;
            }
            const expected = ['recordingId', 'partIndex', 'state', 'containerDurationMs', 'observedAt', 'expiresAt', 'binding', 'observedBytes'];
            if (data.state !== 'container_estimate' || keys.length !== expected.length || keys.some(key => !expected.includes(key)) || !Number.isSafeInteger(data.containerDurationMs) || data.containerDurationMs <= 0 || !Number.isSafeInteger(data.observedBytes) || data.observedBytes <= 0 || typeof data.binding !== 'string' || !/^[a-f0-9]{32}$/.test(data.binding)) throw new Error('Invalid container estimate');
            function utc(value) {
                if (typeof value !== 'string') return NaN;
                const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
                if (!match) return NaN;
                const parsed = new Date(value), timestamp = parsed.getTime();
                if (!Number.isFinite(timestamp) || parsed.getUTCFullYear() !== Number(match[1]) || parsed.getUTCMonth() + 1 !== Number(match[2]) || parsed.getUTCDate() !== Number(match[3]) || parsed.getUTCHours() !== Number(match[4]) || parsed.getUTCMinutes() !== Number(match[5]) || parsed.getUTCSeconds() !== Number(match[6])) return NaN;
                return timestamp;
            }
            const observed = utc(data.observedAt), expires = utc(data.expiresAt), now = clockNow(clock);
            if (!Number.isFinite(observed) || !Number.isFinite(expires) || observed > now || expires <= observed || expires - observed > 15 * 60 * 1000) throw new Error('Invalid observation time');
            return data;
        }
        async function readSmallJSON(response) {
            if (!response.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) throw new Error('Invalid JSON content type');
            const reader = response.body.getReader(); let size = 0, chunks = [];
            try {
                while (true) {
                    const item = await reader.read();
                    if (item.done) break;
                    size += item.value.byteLength;
                    if (size > 8192) { await reader.cancel(); throw new Error('Oversized response'); }
                    chunks.push(item.value);
                }
            } finally { reader.releaseLock(); }
            const bytes = new Uint8Array(size); let offset = 0;
            chunks.forEach(chunk => { bytes.set(chunk, offset); offset += chunk.byteLength; });
            return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
        }
        function showTiming(intent, data, clock) {
            if (!current(intent)) return;
            if (data.state === 'unknown') {
                message(data.reason === 'observation_expired' ? 'Estimate expired. Inspect the selected part explicitly.' : data.reason === 'unsupported_container' || data.reason === 'probe_unavailable' ? 'Estimated part length is unavailable for this part. No duration has been inferred.' : 'No current estimate is stored for this part. Inspect it explicitly if needed.');
                return;
            }
            const expires = Date.parse(data.expiresAt), now = clockNow(clock);
            if (expires <= now) { message('Estimate expired. Inspect the selected part explicitly.'); return; }
            const seconds = data.containerDurationMs / 1000;
            const duration = seconds < 60 ? seconds.toFixed(1) + ' seconds' : Math.floor(seconds / 60) + ' min ' + Math.round(seconds % 60) + ' sec';
            message('Part ' + intent.part + ' estimated part length: ' + duration + '. Container estimate observed ' + new Date(data.observedAt).toLocaleString() + ', valid until ' + new Date(data.expiresAt).toLocaleString() + '. Not an exact media duration or a combined timeline.');
            expiryTimer = setTimeout(() => { if (current(intent)) message('Estimate expired. Inspect the selected part explicitly.'); }, Math.min(expires - now, 15 * 60 * 1000));
        }
        async function requestTiming(intent, token) {
            if (!current(intent) || !['read', 'inspect'].includes(intent.kind)) return;
            const active = new AbortController(); controller = active;
            message(intent.kind === 'read' ? 'Reading the last stored estimate...' : 'Inspecting this one part. Cancel to stop the request.');
            try {
                const started = performance.now();
                const response = await fetch(endpoint(options.apiBase(), '/' + encodeURIComponent(intent.id) + '/parts/' + intent.part + '/timing/' + (intent.kind === 'read' ? 'read' : 'refresh')), {
                    method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: active.signal,
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ profileId: intent.profile, profileUnlock: token || '' })
                });
                if (!current(intent) || controller !== active) return;
                if (!response.ok) {
                    if (response.status === 403) {
                        let denied = {}; try { denied = await readSmallJSON(response); } catch (_) {}
                        if (!current(intent) || controller !== active) return;
                        if (denied.code === 'profile_locked') { unlock = null; controller = null; askPIN(intent); return; }
                    }
                    message(response.status === 429 ? 'Inspection is busy. Try again explicitly later.' : response.status === 409 ? 'This closed part is no longer available for inspection. Reload recordings.' : response.status === 401 || response.status === 403 || response.status === 404 ? 'Part estimate access unavailable. Sign in again or reload recordings.' : 'Part estimate unavailable. Try again explicitly.');
                    return;
                }
                const clock = responseClock(response, started);
                const data = validateTiming(await readSmallJSON(response), intent, clock);
                if (current(intent) && controller === active) showTiming(intent, data, clock);
            } catch (_) { if (current(intent) && controller === active) message('Part estimate unavailable. Try again explicitly.'); }
            finally { if (controller === active) controller = null; }
        }
        options.container.addEventListener('change', function (event) {
            const select = event.target.closest('[data-recording-part]');
            if (!select || !options.container.contains(select)) return;
            const id = select.getAttribute('data-recording-part'), index = Number(select.value), info = details(rows.get(id));
            if (!info || !info.indices.includes(index)) return;
            actionGeneration++; clearDialog(); selections.set(id, index); message('Part selection changed. View or inspect this part explicitly.');
        });
        options.container.addEventListener('click', function (event) {
            if (event.target.closest('[data-recording-timing-cancel]')) { actionGeneration++; clearDialog(); message('Part estimate request cancelled. No new inspection was requested.'); return; }
            const button = event.target.closest('[data-recording-export], [data-recording-timing]');
            if (!button || !options.container.contains(button)) return;
            const exporting = button.hasAttribute('data-recording-export');
            const id = button.getAttribute(exporting ? 'data-recording-export' : 'data-recording-timing'), row = rows.get(id);
            const info = details(row), kind = exporting ? 'export' : button.getAttribute('data-timing-operation');
            if (!['export', 'read', 'inspect'].includes(kind)) return;
            const intent = { id, kind, part: exporting ? null : selectedPart(id), profile: boundProfile, generation: boundGeneration, action: ++actionGeneration };
            if (!current(intent) || !info || !info.exportable || row.userId !== intent.profile) return;
            clearDialog();
            const token = unlock && unlock.profile === intent.profile && unlock.expires > clockNow(unlock.clock) ? unlock.token : '';
            if (!token) unlock = null;
            submit(intent, token);
        });
        function cancelPIN() { actionGeneration++; clearDialog(); message('PIN check cancelled. No new download or inspection was requested.'); }
        dialog.querySelector('[data-export-pin-cancel]').addEventListener('click', cancelPIN);
        dialog.addEventListener('cancel', function (event) { event.preventDefault(); cancelPIN(); });
        dialog.querySelector('form').addEventListener('submit', async function (event) {
            event.preventDefault(); const intent = pending;
            if (!current(intent) || controller) return;
            const pin = pinInput.value; pinInput.value = '';
            if (!pin || pin.length > 256) { pinStatus.textContent = 'Enter the profile PIN.'; return; }
            const active = new AbortController(); controller = active;
            pinStatus.textContent = 'Checking PIN...';
            try {
                const started = performance.now();
                const response = await fetch(endpoint(options.pinBase(), '/' + encodeURIComponent(intent.profile) + '/pin/verify'), {
                    method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: active.signal,
                    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pin })
                });
                const data = await response.json();
                if (!current(intent) || controller !== active) return;
                if (!response.ok || data.valid !== true) { pinStatus.textContent = data.code === 'pin_invalid' || data.code === 'pin_required' ? 'Incorrect PIN. Try again.' : 'PIN access unavailable. Sign in again or reload.'; return; }
                const clock = responseClock(response, started);
                const expires = Date.parse(data.expiresAt), token = data.unlockToken;
                if (typeof token !== 'string' || token.includes(',') || !/^[\x21-\x7e]{1,2048}$/.test(token) || !Number.isFinite(expires) || expires <= clockNow(clock)) throw new Error('Invalid unlock');
                unlock = { profile: intent.profile, token, expires, clock };
                controller = null; clearDialog();
                if (current(intent)) submit(intent, token);
            } catch (_) {
                if (current(intent) && controller === active) pinStatus.textContent = 'PIN check unavailable. Try again explicitly.';
            } finally { if (controller === active) controller = null; }
        });
        return {
            bind: function (items, profile, generation) {
                if (profile !== boundProfile || generation !== boundGeneration) selections.clear();
                rows = new Map(items.filter(row => row && typeof row.id === 'string').map(row => [row.id, row])); boundProfile = profile; boundGeneration = generation;
            },
            selectedPart,
            invalidate: function () {
                actionGeneration++; clearDialog(); unlock = null; selections.clear(); rows.clear(); lastClock = null; boundProfile = ''; boundGeneration = -1;
                frames.forEach(frame => frame.remove()); frames = [];
                message('');
            }
        };
    }
    global.RecordingExportUI = Object.freeze({ details, create });
})(window);
