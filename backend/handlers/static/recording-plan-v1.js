/* Advisory recording forecasts. Scheduling still uses authoritative admission. */
(() => {
    let pending = false;
    let generation = 0;
    let cancelCurrent = null;

    const scope = 'This checks scheduled recordings, including padding. Live viewers, external apps and future rule matches may change demand. No connection is reserved. Recordings wait when capacity is full and do not stop viewers.';

    function label(item) {
        if (!item) return 'Recording capacity forecast unavailable.';
        if (item.state === 'conflict') {
            const at = item.firstConflictStart ? ' from ' + new Date(item.firstConflictStart).toLocaleString() : '';
            return `Recording capacity conflict${at}: up to ${item.peakScheduledConnections} scheduled inputs for ${item.maxStreams} connections.`;
        }
        if (item.state === 'clear') return `No scheduled recording conflict found (${item.peakScheduledConnections}/${item.maxStreams}). Live and external use may still occupy connections.`;
        if (item.state === 'unknown') return 'Recording capacity uncertain: the limit or part of the schedule is unknown.';
        return 'Recording capacity forecast unavailable for this source.';
    }

    function invalidate() {
        generation++;
        if (cancelCurrent) cancelCurrent();
    }

    function ask(item, complete, isCurrent) {
        return new Promise(resolve => {
            const dialog = document.createElement('dialog');
            dialog.setAttribute('aria-label', 'Recording capacity forecast');
            dialog.style.cssText = 'position:fixed;inset:0;margin:auto;box-sizing:border-box;width:calc(100% - 24px);max-width:560px;max-height:85vh;overflow:auto;background:var(--bg-card,#18202c);color:var(--text-primary,#fff);border:1px solid var(--border,#536071);border-radius:12px;padding:20px;';
            const heading = document.createElement('h2');
            heading.textContent = 'Recording capacity forecast';
            heading.style.cssText = 'margin:0 0 12px;font-size:1.25rem;';
            const message = document.createElement('p');
            message.setAttribute('role', 'status');
            message.style.cssText = 'margin:0 0 12px;';
            message.textContent = label(item);
            const detail = document.createElement('p');
            detail.textContent = scope + (complete === false ? ' Some scheduled inputs could not be checked.' : '');
            detail.style.cssText = 'margin:0 0 16px;';
            const age = document.createElement('p');
            age.style.cssText = 'margin:0 0 12px;';
            const actions = document.createElement('div');
            actions.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;';
            const cancel = document.createElement('button');
            cancel.className = 'btn btn-secondary'; cancel.textContent = 'Cancel';
            const retry = document.createElement('button');
            retry.className = 'btn btn-secondary'; retry.textContent = 'Check again';
            const schedule = document.createElement('button');
            schedule.className = 'btn btn-primary'; schedule.textContent = 'Schedule anyway';
            actions.append(cancel, retry, schedule);
            dialog.append(heading, message, detail, age, actions);
            let settled = false;
            let timer;
            const finish = choice => {
                if (settled) return;
                settled = true;
                clearInterval(timer);
                if (cancelCurrent === dismiss) cancelCurrent = null;
                dialog.remove(); resolve(choice);
            };
            const dismiss = () => finish('cancel');
            cancelCurrent = dismiss;
            cancel.onclick = dismiss;
            retry.onclick = () => finish(isCurrent() ? 'retry' : 'cancel');
            const expires = Date.now() + 15000;
            schedule.onclick = () => finish(isCurrent() && Date.now() < expires ? 'schedule' : 'cancel');
            dialog.addEventListener('cancel', event => { event.preventDefault(); dismiss(); });
            dialog.addEventListener('close', dismiss);
            document.body.append(dialog);
            dialog.showModal(); cancel.focus();
            timer = setInterval(() => {
                if (!isCurrent()) { dismiss(); return; }
                if (Date.now() >= expires) {
                    schedule.disabled = true;
                    age.textContent = 'This forecast has expired. Check again before scheduling.';
                }
            }, 150);
        });
    }

    async function confirm(payload, base, fingerprint) {
        if (pending) return false;
        pending = true;
        const token = generation;
        const original = fingerprint();
        const isCurrent = () => {
            try { return token === generation && fingerprint() === original; }
            catch (_) { return false; }
        };
        try {
            while (isCurrent()) {
                const controller = new AbortController();
                const abort = () => controller.abort();
                cancelCurrent = abort;
                const timer = setTimeout(abort, 10000);
                let plan = null;
                try {
                    const response = await fetch(base + '/preview', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(payload), signal: controller.signal
                    });
                    if (response.ok) plan = await response.json();
                    else if (response.status !== 503) throw new Error('Recording preview was rejected. Check your profile, channel and times.');
                } catch (error) {
                    if (!isCurrent()) return false;
                    if (error.message?.startsWith('Recording preview was rejected')) throw error;
                } finally {
                    clearTimeout(timer);
                    if (cancelCurrent === abort) cancelCurrent = null;
                }
                if (!isCurrent()) return false;
                if (plan?.candidate?.state === 'clear') return true;
                const choice = await ask(plan?.candidate, plan?.complete, isCurrent);
                if (choice === 'retry') continue;
                return choice === 'schedule' && isCurrent();
            }
            return false;
        } finally {
            pending = false;
        }
    }
    window.recordingPlanUI = {confirm, invalidate, label};
})();
