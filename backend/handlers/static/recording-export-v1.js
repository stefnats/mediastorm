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
        return { count: capture.parts.length, incomplete: capture.incomplete, exportable: terminal.has(row.status) && allClosed };
    }
    function create(options) {
        const status = options.status;
        const dialog = options.dialog;
        const pinInput = dialog.querySelector('[data-export-pin]');
        const pinStatus = dialog.querySelector('[data-export-pin-status]');
        let rows = new Map(), boundProfile = '', boundGeneration = -1;
        let unlock = null, pending = null, controller = null;
        let frames = [], sequence = 0, actionGeneration = 0;
        function current(intent) {
            return intent && intent.action === actionGeneration && intent.profile === options.profile() && intent.generation === options.generation() && intent.profile === boundProfile && intent.generation === boundGeneration && rows.has(intent.id);
        }
        function endpoint(base, suffix) {
            const url = new URL(base + suffix, location.href);
            if (url.origin !== location.origin || url.username || url.password || url.search || url.hash) throw new Error('Invalid recording endpoint');
            return url.href;
        }
        function message(text) { status.textContent = text; }
        function clearDialog() {
            if (controller) controller.abort();
            controller = null; pending = null; pinInput.value = ''; pinStatus.textContent = '';
            if (dialog.open) dialog.close();
        }
        function askPIN(intent) {
            if (!current(intent)) return;
            clearDialog(); pending = intent;
            message("Unlock this recording profile to request its parts ZIP.");
            dialog.showModal(); pinInput.focus();
        }
        function submit(intent, token) {
            if (!current(intent)) return;
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
        options.container.addEventListener('click', function (event) {
            const button = event.target.closest('[data-recording-export]');
            if (!button || !options.container.contains(button)) return;
            const id = button.getAttribute('data-recording-export'), row = rows.get(id);
            const info = details(row);
            const intent = { id, profile: boundProfile, generation: boundGeneration, action: ++actionGeneration };
            if (!current(intent) || !info || !info.exportable || row.userId !== intent.profile) return;
            clearDialog();
            const token = unlock && unlock.profile === intent.profile && unlock.expires > Date.now() ? unlock.token : '';
            if (!token) unlock = null;
            submit(intent, token);
        });
        function cancelPIN() { actionGeneration++; clearDialog(); message('PIN check cancelled. No new download was requested.'); }
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
                const response = await fetch(endpoint(options.pinBase(), '/' + encodeURIComponent(intent.profile) + '/pin/verify'), {
                    method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: active.signal,
                    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pin })
                });
                const data = await response.json();
                if (!current(intent) || controller !== active) return;
                if (!response.ok || data.valid !== true) { pinStatus.textContent = data.code === 'pin_invalid' || data.code === 'pin_required' ? 'Incorrect PIN. Try again.' : 'PIN access unavailable. Sign in again or reload.'; return; }
                const expires = Date.parse(data.expiresAt), token = data.unlockToken;
                if (typeof token !== 'string' || token.includes(',') || !/^[\x21-\x7e]{1,2048}$/.test(token) || !Number.isFinite(expires) || expires <= Date.now()) throw new Error('Invalid unlock');
                unlock = { profile: intent.profile, token, expires };
                controller = null; clearDialog();
                if (current(intent)) submit(intent, token);
            } catch (_) {
                if (current(intent) && controller === active) pinStatus.textContent = 'PIN check unavailable. Try again explicitly.';
            } finally { if (controller === active) controller = null; }
        });
        return {
            bind: function (items, profile, generation) {
                rows = new Map(items.filter(row => row && typeof row.id === 'string').map(row => [row.id, row])); boundProfile = profile; boundGeneration = generation;
            },
            invalidate: function () {
                actionGeneration++; clearDialog(); unlock = null; rows.clear(); boundProfile = ''; boundGeneration = -1;
                frames.forEach(frame => frame.remove()); frames = [];
                message('');
            }
        };
    }
    global.RecordingExportUI = Object.freeze({ details, create });
})(window);
