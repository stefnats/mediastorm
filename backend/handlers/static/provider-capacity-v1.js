// Fresh account facts may additionally cap new inputs only after global opt-in.
(() => {
    'use strict';
    let active;
    const node = (tag, text) => {
        const element = document.createElement(tag);
        if (text !== undefined) element.textContent = text;
        return element;
    };
    window.MediastormProviderCapacity = {
        open({ basePath, sourceId, sourceName, isCurrent, onUse }) {
            active?.close();
            const dialog = node('dialog');
            active = dialog;
            dialog.setAttribute('aria-label', 'Provider connection limit');
            dialog.style.cssText = 'box-sizing:border-box;margin:auto;max-width:32rem;width:calc(100% - 2rem);max-height:90vh;overflow:auto;padding:1.5rem;border:1px solid var(--border,#444);border-radius:12px;background:var(--bg-secondary,#18181b);color:var(--text-primary,#fafafa);';
            const heading = node('div');
            heading.style.cssText = 'display:flex;align-items:center;justify-content:space-between;gap:1rem;';
            heading.append(node('h2', 'Provider connection limit'));
            const close = node('button', 'Close');
            close.type = 'button'; close.className = 'btn btn-secondary';
            close.onclick = () => dialog.close(); heading.append(close);
            dialog.append(heading, node('p', sourceName));
            const status = node('p', ''); status.setAttribute('role', 'status');
            const facts = node('div'); facts.setAttribute('aria-live', 'polite');
            const buttons = node('div'); buttons.style.cssText = 'display:flex;flex-wrap:wrap;gap:0.75rem;margin-top:1rem;';
            const check = node('button', 'Check provider limit'); check.type = 'button'; check.className = 'btn btn-secondary';
            const use = node('button', 'Use reported limit'); use.type = 'button'; use.className = 'btn btn-primary'; use.disabled = true;
            buttons.append(check, use);
            dialog.append(status, facts, node('p', 'Checking does not save settings or open a stream. If automatic adoption is enabled, a fresh report can additionally cap new connections until it expires. Manual limits stay in place; reports are not refreshed automatically.'), buttons);
            let report = null;
            let loading = false;
            let controller = null;
            let sequence = 0;
            let stale = false;
            let renderedFresh = null;
            const fresh = () => report && Number.isFinite(Date.parse(report.cacheExpiresAt)) && Date.parse(report.cacheExpiresAt) > Date.now();
            const usable = () => !stale && !loading && isCurrent() && fresh() && report.state === 'available' && Number.isInteger(report.reportedLimit) && report.reportedLimit > 0;
            const current = () => {
                if (isCurrent()) return true;
                stale = true; report = null; sequence++; controller?.abort();
                facts.replaceChildren();
                status.textContent = 'The source or settings view changed. Close this dialog and check the saved source again.';
                check.disabled = true; use.disabled = true;
                return false;
            };
            const render = () => {
                facts.replaceChildren();
                if (!report) return;
                renderedFresh = fresh();
                const cap = Number.isInteger(report.configuredLimit) && report.configuredLimit > 0 ? report.configuredLimit : 'Not configured';
                facts.append(node('p', 'Configured limit: ' + cap));
                if (Number.isInteger(report.sharedConfiguredLimit) && report.sharedConfiguredLimit > 0 && report.sharedConfiguredLimit !== report.configuredLimit)
                    facts.append(node('p', 'Another source for this account sets a shared limit of ' + report.sharedConfiguredLimit + '.'));
                if (Number.isInteger(report.reportedLimit) && report.reportedLimit > 0)
                    facts.append(node('p', 'Provider-reported limit: ' + report.reportedLimit));
                if (Number.isInteger(report.providerActiveConnections) && report.providerActiveConnections >= 0)
                    facts.append(node('p', 'Provider-reported active connections: ' + report.providerActiveConnections + (fresh() ? ' (at check)' : ' (expired snapshot)')));
                else
                    facts.append(node('p', 'Provider-reported active connections: Unknown (not available from this check)'));
                if (Number.isInteger(report.localConnections) && report.localConnections >= 0)
                    facts.append(node('p', 'Connections reserved by Mediastorm: ' + report.localConnections));
                facts.append(node('p', 'Connection counts are snapshots. They do not reliably identify usage by other apps.'));
                facts.append(node('p', 'Usage outside Mediastorm: Unknown'));
                facts.append(node('p', report.externalUsage?.reason === 'provider_count_unavailable'
                    ? 'The provider did not supply a usable authenticated active-connection count. Usage by other apps cannot be determined.'
                    : 'Provider counts and local reservations are separate snapshots, including inputs opening or closing. There is no common trusted measurement to identify usage by other apps.'));

                facts.append(node('p', report.automaticAdoptionEnabled
                    ? 'Automatic adoption at check: enabled for fresh checked global-provider reports on the shared account budget. Expired, unavailable or changed-source reports use the manual limit.'
                    : 'Automatic adoption at check: off. Use reported limit copies a value to the form; save settings to change the manual limit.'));
                if (report.enforcementOrigin === 'provider_extra_cap' && Number.isInteger(report.automaticLimit) && report.automaticLimit > 0 && Date.parse(report.automaticExpiresAt) > Date.now())
                    facts.append(node('p', 'Extra cap at check: ' + report.automaticLimit + ' for new connections. Running streams and recordings are retained.'));
                if (Number.isInteger(report.enforcedLimit) && report.enforcedLimit >= 0)
                    facts.append(node('p', 'Local admission cap at check: ' + (report.enforcedLimit > 0 ? report.enforcedLimit : 'Not configured')));

                const checked = Date.parse(report.checkedAt);
                if (Number.isFinite(checked)) facts.append(node('p', 'Provider response check completed: ' + new Date(checked).toLocaleString() + (report.cached ? ' (cached)' : '')));
                const localChecked = Date.parse(report.localSnapshotCompletedAt);
                if (Number.isFinite(localChecked)) facts.append(node('p', 'Local reservation snapshot completed: ' + new Date(localChecked).toLocaleString()));
                facts.append(node('p', 'These are server observation times, not a provider measurement time.'));
                use.disabled = !usable();
            };
            const load = async () => {
                if (loading || !current()) return;
                const request = ++sequence;
                controller = new AbortController(); loading = true; report = null;
                check.disabled = true; use.disabled = true; facts.replaceChildren(); status.textContent = 'Checking the saved provider account…';
                try {
                    const response = await fetch(basePath + '/api/live/provider-capacity', {
                        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ sourceId }), signal: controller.signal,
                    });
                    if (!response.ok) throw new Error(response.status === 409
                        ? 'The saved source changed. Close this dialog, reload settings and check again.'
                        : 'The provider limit could not be checked. Keep a confirmed manual limit or try again.');
                    const result = await response.json();
                    if (!dialog.isConnected || request !== sequence || !current()) return;
                    if (result.sourceId !== sourceId || !['available', 'unknown', 'unsupported', 'unavailable'].includes(result.state)) throw new Error('The server did not confirm this provider report.');
                    report = result;
                    status.textContent = result.message;
                } catch (error) {
                    if (dialog.isConnected && request === sequence && current() && error.name !== 'AbortError') status.textContent = error.message;
                } finally {
                    if (dialog.isConnected && request === sequence) {
                        loading = false; check.disabled = stale; render();
                    }
                }
            };
            check.onclick = load;
            use.onclick = () => {
                if (!current() || !usable()) return;
                if (onUse(report.reportedLimit) !== false) dialog.close();
            };
            const timer = setInterval(() => {
                if (!current()) return;
                use.disabled = !usable();
                if (report && renderedFresh !== fresh()) render();
                if (report?.state === 'available' && !fresh()) status.textContent = 'This report has expired. Check the saved provider again to see the current limit. Reports are not refreshed automatically.';
            }, 250);
            dialog.addEventListener('close', () => {
                sequence++; controller?.abort(); clearInterval(timer); dialog.remove();
                if (active === dialog) active = null;
            }, { once: true });
            document.body.append(dialog); dialog.showModal(); close.focus(); load();
        },
    };
})();
