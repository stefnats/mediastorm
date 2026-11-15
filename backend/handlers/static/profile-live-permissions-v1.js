// Account-managed rights stay separate from personal playback preferences.
(() => {
    'use strict';
    let saving = false;
    let current = null;
    const node = (tag, text, className) => {
        const element = document.createElement(tag);
        if (text !== undefined) element.textContent = text;
        if (className) element.className = className;
        return element;
    };
    window.MediastormLivePermissions = {
        open({ profile, basePath, onSaved, showMessage }) {
            if (!profile) return;
            if (saving) {
                showMessage?.('The current IPTV permission change is still saving.', 'error');
                return;
            }
            current?.close();
            const dialog = node('dialog');
            current = dialog;
            dialog.setAttribute('aria-label', 'IPTV takeover rights');
            dialog.style.cssText = 'box-sizing:border-box;margin:auto;max-width:30rem;width:calc(100% - 2rem);max-height:90vh;overflow:auto;padding:1.5rem;border:1px solid var(--border,#444);border-radius:var(--radius-lg,12px);background:var(--bg-secondary,#18181b);color:var(--text-primary,#fafafa);';
            const heading = node('div');
            heading.style.cssText = 'display:flex;justify-content:space-between;align-items:center;gap:1rem;';
            heading.append(node('h2', 'IPTV takeover rights'));
            const close = node('button', 'Close', 'btn btn-secondary');
            close.type = 'button';
            close.onclick = () => dialog.close();
            heading.append(close);
            dialog.append(heading, node('p', profile.name), node('p', 'These rights apply only within this account. Both are off by default.'));
            const form = node('form');
            const group = node('fieldset');
            group.style.cssText = 'border:0;padding:0;margin:1rem 0;min-width:0;';
            group.append(node('legend', 'Allowed actions', 'form-label'));
            const checkbox = (name, title, explanation, checked) => {
                const label = node('label');
                label.style.cssText = 'display:block;margin:1rem 0;line-height:1.5;';
                const input = node('input');
                input.type = 'checkbox'; input.name = name; input.checked = Boolean(checked);
                input.style.cssText = 'width:1.1rem;height:1.1rem;margin-right:0.6rem;vertical-align:middle;';
                label.append(input, node('span', title));
                const hint = node('span', explanation, 'form-hint');
                hint.style.display = 'block';
                label.append(hint);group.append(label);
                return input;
            };
            const live = checkbox('allowLiveTakeover', 'Take over a live IPTV place', 'When all places are occupied, Play may stop the oldest eligible live input in this account. Every viewer sharing that input is affected.', profile.allowLiveTakeover);
            const recordings = checkbox('allowRecordingInterruption', 'Allow stopping an IPTV recording', 'A running recording still needs an explicit warning and confirmation each time. Its recorded portion is kept as incomplete.', profile.allowRecordingInterruption);
            const unavailable = Boolean(profile.isKidsProfile) || !profile.accountId;
            group.disabled = unavailable;
            form.append(group);
            if (unavailable) form.append(node('p', 'Takeover rights require an adult profile assigned to an account.', 'form-hint'));
            const status = node('p', '');status.setAttribute('role', 'status');status.style.minHeight = '1.5em';form.append(status);
            const submit = node('button', 'Save IPTV rights', 'btn btn-primary');
            submit.type = 'submit';submit.disabled = unavailable;form.append(submit);
            form.onsubmit = async event => {
                event.preventDefault();
                if (saving || unavailable) return;
                saving = true;group.disabled = true;submit.disabled = true;status.textContent = 'Saving IPTV rights…';
                try {
                    const response = await fetch(basePath + '/api/profiles/live-permissions?profileId=' + encodeURIComponent(profile.id), {
                        method: 'PUT', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ allowLiveTakeover: live.checked, allowRecordingInterruption: recordings.checked }),
                    });
                    if (!response.ok) throw new Error('IPTV rights could not be saved. Confirm account access and try again.');
                    const result = await response.json();
                    if (typeof result.allowLiveTakeover !== 'boolean' || typeof result.allowRecordingInterruption !== 'boolean') throw new Error('The server did not confirm the saved rights. Try again.');
                    profile.allowLiveTakeover = result.allowLiveTakeover;
                    profile.allowRecordingInterruption = result.allowRecordingInterruption;
                    dialog.close();onSaved?.();
                } catch (error) {
                    if (dialog.isConnected) status.textContent = error.message || 'The change could not be confirmed. Try again.';
                } finally {
                    saving = false;
                    if (dialog.isConnected) { group.disabled = unavailable; submit.disabled = unavailable; }
                }
            };
            dialog.append(form);
            dialog.addEventListener('close', () => { if (current === dialog) current = null; dialog.remove(); }, { once: true });
            document.body.append(dialog);dialog.showModal();close.focus();
        },
    };
})();
