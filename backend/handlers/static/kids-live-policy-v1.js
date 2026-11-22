// The existing parent confirmation helper asks for the account password. This
// form edits one dedicated Live rule; it does not change movie/episode limits.
(() => {
    'use strict';
    const form = document.getElementById('kids-live-policy-form');
    if (!form) return;
    const fields = document.getElementById('kids-live-fields');
    const enabled = document.getElementById('kids-live-enabled');
    const age = document.getElementById('kids-live-age');
    const guide = document.getElementById('kids-live-guide');
    const status = document.getElementById('kids-live-status');
    const reload = document.getElementById('kids-live-reload');
    const id = new URLSearchParams(location.search).get('profileId');
    const endpoint = basePath + '/api/users/' + encodeURIComponent(id || '') + '/kids/live-policy';
    let pending = null;
    let loaded = false;
    let retired = false;
    const policy = () => ({enabled: enabled.checked, country: 'DE', maximumAge: Number(age.value), guideSourceId: guide.value});
    const current = action => pending === action && !retired && !action.signal.aborted && form.isConnected && new URLSearchParams(location.search).get('profileId') === id;
    const controls = () => { fields.disabled = !loaded || pending !== null || retired; reload.disabled = pending !== null || retired; };
    const message = text => { status.textContent = text; };
    function display(value) {
        const p = value?.policy;
        if (!p || typeof p.enabled !== 'boolean' || p.country !== 'DE' || ![0, 6, 12, 16, 18].includes(p.maximumAge) || typeof p.guideSourceId !== 'string' || !Array.isArray(value.guideSources)) throw new Error('Invalid saved Live TV rule');
        guide.replaceChildren(new Option('Choose a loaded guide', ''));
        for (const source of value.guideSources) {
            if (!source || typeof source.id !== 'string' || typeof source.type !== 'string') throw new Error('Invalid loaded guide');
            guide.add(new Option((source.name || source.type) + ' · ' + source.id.slice(0, 12), source.id));
        }
        if (p.guideSourceId && ![...guide.options].some(option => option.value === p.guideSourceId)) guide.add(new Option('Saved guide is not currently loaded', p.guideSourceId));
        enabled.checked = p.enabled;
        age.value = String(p.maximumAge);
        guide.value = p.guideSourceId;
    }
    async function load() {
        pending?.abort();
        const action = new AbortController();
        pending = action;
        loaded = false;
        controls();
        message('Reading the saved Live TV rule…');
        try {
            if (!id) throw new Error('A profile is required');
            const response = await fetch(endpoint, {signal: action.signal, cache: 'no-store'});
            if (!current(action)) return;
            if (!response.ok) throw new Error('Live TV settings could not be loaded');
            const value = await response.json();
            if (!current(action)) return;
            display(value);
            loaded = true;
            message(value.policy.enabled ? 'Saved rule loaded. Playback uses only the selected guide.' : 'Live TV is off for this profile.');
        } catch (error) {
            if (current(action) && error.name !== 'AbortError') message('Live TV settings are unavailable. Reload before changing the rule.');
        } finally {
            if (pending === action) { pending = null; controls(); }
        }
    }
    form.addEventListener('submit', async event => {
        event.preventDefault();
        if (!loaded || pending || retired || typeof window.parentModeFetch !== 'function') return;
        const value = policy();
        if (value.enabled && !value.guideSourceId) { message('Choose a loaded guide before enabling Live TV.'); return; }
        const action = new AbortController();
        const body = JSON.stringify(value);
        pending = action;
        controls();
        message('Confirm and save this Live TV rule…');
        try {
            const response = await window.parentModeFetch(endpoint, {method: 'PUT', headers: {'Content-Type': 'application/json'}, body, signal: action.signal}, () => current(action) && JSON.stringify(policy()) === body);
            if (!current(action)) return;
            if (!response.ok) {
                if (response.status >= 500) loaded = false;
                message(response.status === 403 ? 'Parent confirmation was not completed. Nothing was enabled here.' : 'The rule was not confirmed. Reload its saved state before trying again.');
                return;
            }
            const saved = await response.json();
            if (!current(action)) return;
            display(saved);
            if (JSON.stringify(policy()) !== body) throw new Error('The saved rule differs');
            message(value.enabled ? 'Live TV rule saved. Unknown and higher-rated programmes remain blocked.' : 'Live TV is now off for this profile.');
        } catch (error) {
            if (current(action) && error.name !== 'AbortError') { loaded = false; message('The save result is unknown. Reload the saved rule; no save will be repeated automatically.'); }
        } finally {
            if (pending === action) { pending = null; controls(); }
        }
    });
    reload.addEventListener('click', load);
    document.getElementById('reload-profile-state')?.addEventListener('click', load);
    window.addEventListener('pagehide', () => { retired = true; pending?.abort(); controls(); });
    load();
})();
