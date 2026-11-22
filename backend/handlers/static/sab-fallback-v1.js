// Saved observations only. A download observation never grants playback/import.
(function () {
  'use strict';
  const panel = document.getElementById('sabFallbackPanel');
  if (!panel) return;
  const rows = document.getElementById('sabFallbackRows');
  const message = document.getElementById('sabFallbackMessage');
  const reload = document.getElementById('sabFallbackReload');
  const previous = document.getElementById('sabFallbackPrevious');
  const next = document.getElementById('sabFallbackNext');
  const states = Object.freeze({
    submission_unknown: 'Submission acknowledgement unknown', submitted: 'Handed off',
    queued: 'Download queued', downloading: 'Downloading',
    download_completed: 'Download completed; import not confirmed', failed: 'Download failed',
    unknown: 'Download status unknown', not_submitted: 'No handoff recorded'
  });
  const reasons = Object.freeze({
    acknowledgement_unavailable: 'No acknowledgement was recorded. This view cannot resubmit.',
    provider_unavailable: 'The last explicit status check could not reach the provider.',
    provider_state_unknown: 'The provider did not return a known status.'
  });
  const endpoint = new URL(panel.dataset.endpoint, location.href);
  if (endpoint.origin !== location.origin || endpoint.search || endpoint.hash) return;
  let generation = 0, retired = false, busy = false, cursor = '', nextCursor = '', history = [];
  const pending = new Set();
  const actions = new WeakMap();
  function action(card) { const value = {}; actions.set(card, value); return value; }
  function owns(card, stamp, value) { return isCurrent(stamp) && card.isConnected && actions.get(card) === value; }
  function controls() {
    reload.disabled = retired || busy;
    previous.disabled = retired || busy || history.length === 0;
    next.disabled = retired || busy || !nextCursor;
  }
  function cancel() { for (const request of pending) request.abort(); pending.clear(); }
  function terminal() {
    retired = true; generation++; cancel(); rows.replaceChildren(); nextCursor = ''; history = [];
    message.textContent = 'Access is no longer available. Reload the page after signing in.'; controls();
  }
  function isCurrent(stamp) { return !retired && stamp === generation && panel.isConnected; }
  function keys(value, allowed) {
    return value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).every(key => allowed.includes(key));
  }
  function observation(value) {
    if (!keys(value, ['queueId', 'state', 'reason', 'submittedAt', 'checkedAt']) || !Number.isSafeInteger(value.queueId) || value.queueId <= 0 || !Object.hasOwn(states, value.state)) throw Error('shape');
    if (value.reason !== undefined && value.reason !== '' && !Object.hasOwn(reasons, value.reason)) throw Error('shape');
    for (const key of ['submittedAt', 'checkedAt']) {
      if (value[key] !== undefined && (typeof value[key] !== 'string' || value[key].length > 64 || !Number.isFinite(Date.parse(value[key])))) throw Error('shape');
    }
    return value;
  }
  async function boundedJSON(reply) {
    const reader = reply.body.getReader();
    let used = 0;
    const chunks = [];
    try {
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        used += part.value.byteLength;
        if (used > 524288) { await reader.cancel(); throw Error('shape'); }
        chunks.push(part.value);
      }
    } finally { reader.releaseLock(); }
    const bytes = new Uint8Array(used);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    return JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(bytes));
  }
  async function request(url, method, stamp, body) {
    const controller = new AbortController(); pending.add(controller);
    try {
      const headers = {Accept: 'application/json'};
      if (body !== undefined) headers['Content-Type'] = 'application/json';
      const reply = await fetch(url, {method, credentials: 'same-origin', cache: 'no-store', redirect: 'error', headers, body: body === undefined ? undefined : JSON.stringify(body), signal: controller.signal});
      if (!isCurrent(stamp)) throw Error('stale');
      if (reply.status === 401 || reply.status === 403) { terminal(); throw Error('terminal'); }
      if (!reply.ok) {
        const error = Error('request');
        error.status = reply.status;
        try {
          const body = await boundedJSON(reply);
          if (body && ['copy_already_recorded', 'copy_budget_exceeded', 'copy_outcome_unknown', 'fallback_busy'].includes(body.code)) error.code = body.code;
        } catch (_) { /* Errors remain fixed local messages. */ }
        throw error;
      }
      if (!(reply.headers.get('content-type') || '').includes('application/json')) throw Error('shape');
      const value = await boundedJSON(reply);
      if (!isCurrent(stamp)) throw Error('stale');
      return value;
    } finally { pending.delete(controller); }
  }
  function text(tag, value) { const node = document.createElement(tag); node.textContent = value; return node; }
  function draw(item, card, stamp) {
    action(card);
    card.replaceChildren(text('h3', 'Queue ' + item.queueId), text('p', states[item.state]));
    if (item.reason) card.append(text('p', reasons[item.reason]));
    for (const [key, label] of [['submittedAt', 'Handoff observed'], ['checkedAt', 'Status checked']]) {
      card.append(text('p', label + ': ' + (item[key] ? new Date(item[key]).toLocaleString() : 'Not recorded')));
    }
    const refresh = text('button', 'Refresh status'); refresh.type = 'button'; refresh.className = 'btn btn-secondary';
    const status = text('p', ''); status.setAttribute('role', 'status');
    refresh.addEventListener('click', async () => {
      if (!isCurrent(stamp) || refresh.disabled) return;
      const intent = action(card);
      const paused = Array.from(card.querySelectorAll('button,input,select'), node => [node, node.disabled]);
      for (const [node] of paused) node.disabled = true;
      status.textContent = 'Checking this existing handoff…';
      try {
        const value = observation(await request(endpoint.href + '/' + item.queueId + '/refresh', 'POST', stamp));
        if (value.queueId !== item.queueId) throw Error('shape');
        if (owns(card, stamp, intent)) draw(value, card, stamp);
      } catch (_) {
        if (owns(card, stamp, intent)) status.textContent = 'Status could not be refreshed. Saved observation is unchanged; try Refresh status again.';
      } finally { if (owns(card, stamp, intent)) for (const [node, disabled] of paused) if (node.isConnected) node.disabled = disabled; }
    });
    card.append(refresh, status);
    if (panel.dataset.arrImport === 'true') arrControls(item, card, stamp);
    if (panel.dataset.managedCopy === 'true') copyControls(item, card, stamp, refresh);
    if (panel.dataset.importPreview === 'true' && item.state === 'download_completed') previewControls(item, card, stamp, refresh);
  }
  function arrControls(item, card, stamp) {
    const section = document.createElement('div');
    section.append(text('h4', 'Sonarr / Radarr library import'));
    const backend = document.createElement('select'); backend.setAttribute('aria-label', 'Import app');
    const view = text('button', 'View import'); view.type = 'button'; view.className = 'btn btn-secondary';
    const submit = text('button', 'Import download'); submit.type = 'button'; submit.className = 'btn btn-secondary'; submit.disabled = true;
    const refresh = text('button', 'Check import / retry library scan'); refresh.type = 'button'; refresh.className = 'btn btn-secondary'; refresh.disabled = true;
    const state = text('p', ''); state.setAttribute('role', 'status');
    const library = text('a', 'Open local library'); library.href = panel.dataset.arrLibraryHref; library.hidden = true;
    const labels = {not_started:'No library import yet.', submission_unknown:'Submission acknowledgement unknown. Check the import before retrying.', importing:'The app is importing this download.', import_failed:'The app could not import this download. Check its queue and paths, then retry.', awaiting_files:'Waiting for the imported library file.', media_unavailable:'The imported file could not be verified as video. Check the file and ffprobe configuration, then Check import again.', needs_match:'The file is in the local library. Assign its title in library management.', library_scan_failed:'The import was reported, but the local library scan failed. Check import retries the scan.', configuration_changed:'The configured app or path changed. Review settings before retrying.', available:'Available in your local library. Your normal playback source preference applies.'};
    let current = null, working = false;
    function render(value) {
      if (!value || value.queueId !== item.queueId || !Object.hasOwn(labels, value.state) || !Array.isArray(value.itemIds)) throw Error('shape');
      current = value; state.textContent = labels[value.state];
      if (value.state === 'available') state.textContent += ' Indexed files: ' + value.itemIds.length + '.';
      if (value.reason === 'provider_unavailable') state.textContent += ' The app is currently unreachable; try Check import again.';
      if (value.reason === 'multiple_episodes') state.textContent += ' This file contains multiple episodes. Review the manual episode selection in the local library.';
      if (value.reason === 'manual_match_conflict') state.textContent += ' Your manual title or episode differs from the app history. It has been preserved; review it in the local library.';
      if (value.reason === 'library_file_missing') state.textContent += ' Verify the configured library mount/path mapping.';
      if (value.reason === 'no_imported_file') state.textContent += ' The command finished without a confirmed imported file. Review the app’s import queue.';
      backend.replaceChildren();
      for (const kind of value.configuredBackends || []) if (kind === 'sonarr' || kind === 'radarr') { const option = text('option', kind === 'sonarr' ? 'Sonarr' : 'Radarr'); option.value = kind; backend.append(option); }
      if (value.backend) backend.value = value.backend;
      if (!backend.options.length) state.textContent += ' Configure Sonarr or Radarr and its local library in Settings.';
      const retry = ['import_failed', 'submission_unknown', 'configuration_changed'].includes(value.state);
      submit.textContent = retry ? 'Retry import' : 'Import download';
      submit.disabled = working || !backend.value || item.state !== 'download_completed' || value.state !== 'not_started' && !retry;
      backend.disabled = working || value.state !== 'not_started' && !retry;
      refresh.disabled = working || value.state === 'not_started';
      library.hidden = !['available', 'needs_match', 'library_scan_failed', 'media_unavailable'].includes(value.state);
    }
    async function perform(actionName) {
      if (working || !isCurrent(stamp)) return;
      if (actionName === 'submit' && !window.confirm(current && current.state !== 'not_started' ? 'Retry this copy-only import? Check the app first if the previous acknowledgement was unknown.' : 'Send this completed download to the selected app for a copy-only library import?')) return;
      working = true; let failed = false;
      const currentSection = () => isCurrent(stamp) && section.isConnected;
      view.disabled = submit.disabled = refresh.disabled = backend.disabled = true;
      const url = endpoint.href + '/' + item.queueId + '/arr';
      try {
        const value = await request(actionName === 'refresh' ? url + '/refresh' : url, actionName === 'status' ? 'GET' : 'POST', stamp, actionName === 'submit' ? {backend: backend.value, retry: current.state !== 'not_started'} : undefined);
        if (currentSection()) { working = false; render(value); }
      } catch (_) { failed = true; }
      finally { working = false; if (currentSection()) { view.disabled = false; if (current) render(current); if (failed) state.textContent = 'The import could not be checked. Saved status is retained. Try View import or Check import again.'; } }
    }
    view.addEventListener('click', () => perform('status')); submit.addEventListener('click', () => perform('submit')); refresh.addEventListener('click', () => perform('refresh'));
    backend.addEventListener('change', () => { if (current && ['not_started', 'import_failed', 'submission_unknown', 'configuration_changed'].includes(current.state)) submit.disabled = !backend.value || item.state !== 'download_completed'; });
    section.append(view, backend, submit, refresh, state, library); card.append(section);
  }
  function copyTime(value) {
    if (typeof value !== 'string') return false;
    const match = /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.\d{1,9})?Z$/.exec(value);
    if (!match || !Number.isFinite(Date.parse(value))) return false;
    const date = new Date(value);
    return Number(match[1]) >= 1 && date.getUTCFullYear() === Number(match[1]) && date.getUTCMonth() + 1 === Number(match[2]) && date.getUTCDate() === Number(match[3]) && date.getUTCHours() === Number(match[4]) && date.getUTCMinutes() === Number(match[5]) && date.getUTCSeconds() === Number(match[6]);
  }
  function savedCopy(value, id) {
    if (!keys(value, ['queueId', 'state', 'configured', 'reason', 'chargedBytes', 'fileCount', 'observedAt']) || value.queueId !== id || typeof value.configured !== 'boolean' || !['unknown', 'not_recorded', 'copy_recorded', 'reconciliation_required'].includes(value.state)) throw Error('shape');
    if (value.reason !== undefined && !['not_configured', 'namespace_unobserved', 'configuration_changed', 'attempt_unconfirmed'].includes(value.reason)) throw Error('shape');
    if (value.state === 'unknown') {
      if (value.chargedBytes !== undefined || value.fileCount !== undefined || value.observedAt !== undefined || !['not_configured', 'namespace_unobserved', 'configuration_changed'].includes(value.reason)) throw Error('shape');
      if (value.configured !== (value.reason !== 'not_configured')) throw Error('shape');
    } else {
      if (!value.configured || !Number.isSafeInteger(value.chargedBytes) || value.chargedBytes < 0 || value.chargedBytes > 1099511627776 || !Number.isSafeInteger(value.fileCount) || value.fileCount < 0 || value.fileCount > 128) throw Error('shape');
      if (value.state === 'not_recorded') {
        if (value.reason !== undefined || value.chargedBytes !== 0 || value.fileCount !== 0 || value.observedAt !== undefined) throw Error('shape');
      } else {
        if (value.fileCount === 0 || !copyTime(value.observedAt)) throw Error('shape');
        if (value.state === 'copy_recorded' && value.reason !== undefined || value.state === 'reconciliation_required' && value.reason !== 'attempt_unconfirmed') throw Error('shape');
      }
    }
    return value;
  }
  function copyControls(item, card, stamp, refresh) {
    const view = text('button', 'View saved copy'); view.type = 'button'; view.className = 'btn btn-secondary';
    const copy = text('button', 'Copy files'); copy.type = 'button'; copy.disabled = true; copy.className = 'btn btn-secondary';
    const state = text('p', ''); state.setAttribute('role', 'status'); state.setAttribute('aria-live', 'polite');
    let current = null;
    function hideCopy(hidden) { copy.hidden = hidden; copy.style.display = hidden ? 'none' : ''; }
    hideCopy(true);
    const url = new URL(endpoint.href.replace(/\/$/, '') + '/' + item.queueId + '/stage');
    function eligible() { return item.state === 'download_completed' && current && current.configured && (current.state === 'not_recorded' || current.state === 'unknown' && current.reason === 'namespace_unobserved'); }
    function controls(waiting) {
      view.disabled = waiting; refresh.disabled = waiting; copy.disabled = waiting || !eligible();
      const preview = card.querySelector('[data-sab-import-preview]');
      if (preview) preview.disabled = waiting;
      const publication = card.querySelector('[data-sab-library-publication]');
      if (publication) publication.inert = waiting;
    }
    function show(value) {
      current = value;
      const labels = {unknown: 'Saved copy status is unknown.', not_recorded: 'No saved copy is recorded for this location.', copy_recorded: 'A saved copy was recorded. This does not mean ready to play.', reconciliation_required: 'This copy needs checking. It cannot be copied again here.'};
      state.textContent = value.reason === 'not_configured' ? 'Copy files is not set up.' : value.reason === 'configuration_changed' ? 'The download settings have changed. Saved copy status is unknown.' : labels[value.state];
      if (value.chargedBytes !== undefined) state.append(text('span', ' Counted toward the storage limit: ' + value.chargedBytes.toLocaleString() + ' bytes.'));
      hideCopy(!eligible()); controls(false);
      if (panel.dataset.libraryPublication === 'true') publicationControls(item, card, stamp, value);
    }
    view.addEventListener('click', async () => {
      if (view.disabled || !isCurrent(stamp) || !card.isConnected) return;
      const intent = action(card); controls(true); current = null; hideCopy(true); state.textContent = 'Loading saved copy status…';
      try { const value = savedCopy(await request(url, 'GET', stamp), item.queueId); if (owns(card, stamp, intent)) show(value); }
      catch (_) { if (owns(card, stamp, intent)) { state.textContent = 'Saved copy status could not be loaded.'; controls(false); } }
    });
    copy.addEventListener('click', async () => {
      if (copy.disabled || !eligible() || !isCurrent(stamp) || !card.isConnected) return;
      if (!window.confirm('Copy these files to the current managed storage? This may take a few minutes.')) return;
      const intent = action(card); controls(true); state.textContent = 'Copying files…';
      try { const value = savedCopy(await request(url, 'POST', stamp), item.queueId); if (owns(card, stamp, intent)) show(value); }
      catch (error) {
        if (owns(card, stamp, intent)) {
          current = null; hideCopy(true);
          const labels = {copy_already_recorded: 'A copy request is already recorded. View saved copy to check it.', copy_budget_exceeded: 'The storage limit would be exceeded. No new copy was started.', fallback_busy: 'Copy files is busy. View saved copy before trying again.'};
          state.textContent = labels[error.code] || 'The copy could not be confirmed and may be partial. View saved copy to check it.'; controls(false);
        }
      }
    });
    card.append(view, copy, state);
  }
  function publicationControls(item, card, stamp, saved) {
    const old = card.querySelector('[data-sab-library-publication]');
    if (old) old.remove();
    if (saved.state !== 'copy_recorded' || saved.fileCount < 1) return;
    const box = document.createElement('fieldset'); box.dataset.sabLibraryPublication = 'true'; box.style.minInlineSize = '0';
    box.append(text('legend', 'Add saved files to a library'));
    const load = text('button', 'Choose library'); load.type = 'button'; load.className = 'btn btn-secondary';
    const check = text('button', 'Check library import'); check.type = 'button'; check.className = 'btn btn-secondary';
    const label = text('label', 'Target library ');
    const select = document.createElement('select'); select.setAttribute('aria-label', 'Target library'); select.style.maxWidth = '100%'; select.disabled = true; label.append(select);
    const inspect = text('button', 'Inspect saved files'); inspect.type = 'button'; inspect.disabled = true; inspect.className = 'btn btn-secondary';
    const files = document.createElement('div');
    const publish = text('button', 'Add selected files'); publish.type = 'button'; publish.disabled = true; publish.className = 'btn btn-primary';
    const reconcile = text('button', 'Verify recorded import'); reconcile.type = 'button'; reconcile.disabled = true; reconcile.className = 'btn btn-secondary';
    const state = text('p', 'Choose a library and inspect the saved files before adding them.'); state.setAttribute('role', 'status'); state.setAttribute('aria-live', 'polite');
    box.append(load, check, label, inspect, files, publish, reconcile, state); card.append(box);
    const base = endpoint.href.replace(/\/$/, '') + '/' + item.queueId + '/publication';
    const librariesURL = new URL(panel.dataset.libraryEndpoint, location.href);
    if (librariesURL.origin !== location.origin || librariesURL.search || librariesURL.hash) { box.remove(); return; }
    let libraries = [], record = null, knownEmpty = false, previewLibrary = '', eligible = [], working = false;
    const all = Array.from({length: saved.fileCount}, (_, i) => i + 1);
    function ordinals(value) {
      if (!Array.isArray(value) || value.length < 1 || value.length > 128) throw Error('shape');
      let previous = 0;
      for (const ordinal of value) {
        if (!Number.isSafeInteger(ordinal) || ordinal <= previous || ordinal > saved.fileCount) throw Error('shape');
        previous = ordinal;
      }
      return value;
    }
    function publication(value) {
      const allowed = ['reference', 'state', 'bytes', 'files', 'libraryId', 'ordinals'];
      const states = ['claimed', 'files_prepared', 'files_durable', 'registration_unknown', 'reconciliation_required', 'registered'];
      if (!keys(value, allowed) || typeof value.reference !== 'string' || !/^[0-9a-f]{32}$/.test(value.reference) || !states.includes(value.state) || typeof value.libraryId !== 'string' || !value.libraryId || value.libraryId.length > 256 || !Number.isSafeInteger(value.bytes) || value.bytes < 1 || value.bytes > 1099511627776) throw Error('shape');
      ordinals(value.ordinals);
      if (value.files !== undefined) {
        if (!Array.isArray(value.files) || value.files.length !== value.ordinals.length) throw Error('shape');
        let sum = 0;
        value.files.forEach((file, i) => {
          if (!keys(file, ['ordinal', 'itemId', 'bytes', 'container', 'matchStatus']) || file.ordinal !== value.ordinals[i] || typeof file.itemId !== 'string' || !file.itemId || file.itemId.length > 256 || !Number.isSafeInteger(file.bytes) || file.bytes < 1 || file.bytes > 8589934592 || file.container !== undefined && !['matroska', 'mov', 'mpegts'].includes(file.container) || !['unmatched', 'matched', 'manual', 'low_confidence'].includes(file.matchStatus)) throw Error('shape');
          sum += file.bytes;
        });
        if (sum !== value.bytes) throw Error('shape');
      }
      if (value.state === 'registered' && !value.files) throw Error('shape');
      return value;
    }
    function selected() {
      return Array.from(files.querySelectorAll('input:checked'), input => Number(input.value)).sort((a, b) => a - b);
    }
    function sync() {
      if (working) return;
      select.disabled = libraries.length === 0 || record !== null;
      inspect.disabled = !knownEmpty || !select.value;
      publish.disabled = !knownEmpty || !previewLibrary || previewLibrary !== select.value || selected().length === 0;
      reconcile.disabled = !record || !['files_durable', 'registration_unknown'].includes(record.state);
    }
    function resetPreview() { previewLibrary = ''; eligible = []; files.replaceChildren(); sync(); }
    function paused() {
      working = true;
      const controls = Array.from(card.querySelectorAll('button,input,select'), node => [node, node.disabled]);
      for (const [node] of controls) node.disabled = true;
      return () => { working = false; for (const [node, disabled] of controls) if (node.isConnected) node.disabled = disabled; sync(); };
    }
    async function currentRecord(current) {
      try { return publication(await request(base, 'GET', current)); }
      catch (error) { if (error.status === 404) return null; throw error; }
    }
    function showRecord(value) {
      record = value; knownEmpty = value === null; resetPreview();
      if (!value) { state.textContent = 'No library import is recorded. Select a target and inspect the saved files.'; return; }
      select.value = value.libraryId;
      const labels = {claimed: 'An import is recorded, but the copied files are not confirmed. Check the recorded operation before taking further action.', files_prepared: 'Files were prepared, but their complete copy is not confirmed. Manual checking is required.', files_durable: 'The copied files are saved. Verify the recorded import to check and finish library registration.', registration_unknown: 'Library registration could not be confirmed. Verify the recorded import before taking further action.', reconciliation_required: 'This import needs manual checking.', registered: 'The files were added to the library. Review their title matches in Libraries.'};
      state.textContent = labels[value.state];
      if (value.files) for (const file of value.files) files.append(text('p', 'File ' + file.ordinal + ': ' + (file.matchStatus === 'unmatched' ? 'Added; title not matched.' : 'Added.')));
      sync();
    }
    async function loadStatus(withLibraries) {
      if (!isCurrent(stamp) || !box.isConnected) return;
      const intent = action(card), restore = paused();
      knownEmpty = false; resetPreview(); state.textContent = 'Checking the saved library import…';
      try {
        if (withLibraries) {
          const value = await request(librariesURL, 'GET', stamp);
          if (!Array.isArray(value) || value.length > 256) throw Error('shape');
          const local = value.filter(library => library && library.sourceType === 'local');
          const seen = new Set();
          for (const library of local) {
            if (typeof library.id !== 'string' || !library.id || library.id.length > 256 || seen.has(library.id) || typeof library.name !== 'string' || library.name.length > 4096) throw Error('shape');
            seen.add(library.id);
          }
          if (!owns(card, stamp, intent)) return;
          libraries = local; select.replaceChildren();
          for (const library of local) { const option = text('option', library.name); option.value = library.id; select.append(option); }
        }
        const value = await currentRecord(stamp);
        if (!owns(card, stamp, intent)) return;
        showRecord(value);
        if (!libraries.length && !record) state.textContent = 'Create a local library in Libraries, then choose it here.';
      } catch (_) {
        if (owns(card, stamp, intent)) { record = null; knownEmpty = false; state.textContent = 'The library import could not be checked. Use Check library import before taking further action.'; }
      } finally { if (owns(card, stamp, intent)) restore(); }
    }
    load.addEventListener('click', () => { if (!load.disabled) loadStatus(true); });
    check.addEventListener('click', () => { if (!check.disabled) loadStatus(libraries.length === 0); });
    select.addEventListener('change', () => { action(card); resetPreview(); });
    inspect.addEventListener('click', async () => {
      if (inspect.disabled || !knownEmpty || !isCurrent(stamp)) return;
      const libraryId = select.value, intent = action(card), restore = paused();
      resetPreview(); state.textContent = 'Inspecting the current saved files…';
      try {
        const value = await request(base + '/preview', 'POST', stamp, {libraryId, ordinals: all});
        if (!keys(value, ['queueId', 'libraryId', 'files']) || value.queueId !== item.queueId || value.libraryId !== libraryId || !Array.isArray(value.files) || value.files.length !== all.length) throw Error('shape');
        value.files.forEach((file, i) => {
          if (!keys(file, ['ordinal', 'bytes', 'container', 'eligible']) || file.ordinal !== all[i] || !Number.isSafeInteger(file.bytes) || file.bytes < 0 || file.bytes > 8589934592 || typeof file.eligible !== 'boolean' || file.eligible && !['matroska', 'mov', 'mpegts'].includes(file.container) || !file.eligible && file.container !== undefined) throw Error('shape');
        });
        if (!owns(card, stamp, intent)) return;
        previewLibrary = libraryId; eligible = value.files.filter(file => file.eligible).map(file => file.ordinal);
        for (const file of value.files) {
          const row = document.createElement('label'), input = document.createElement('input');
          input.type = 'checkbox'; input.value = String(file.ordinal); input.checked = file.eligible; input.disabled = !file.eligible;
          input.addEventListener('change', sync);
          row.append(input, document.createTextNode(' File ' + file.ordinal + ': ' + (file.eligible ? ({matroska: 'Matroska', mov: 'MP4', mpegts: 'MPEG-TS'})[file.container] : 'Format not recognized') + ', ' + file.bytes.toLocaleString() + ' bytes.'));
          files.append(row, document.createElement('br'));
        }
        state.textContent = eligible.length ? 'Choose the files to add. Titles are not matched automatically.' : 'No supported video container was recognized in these saved files.';
      } catch (_) {
        if (owns(card, stamp, intent)) { resetPreview(); state.textContent = 'The saved files could not be inspected. No library import was requested.'; }
      } finally { if (owns(card, stamp, intent)) restore(); }
    });
    async function mutate(recover) {
      if (!isCurrent(stamp) || !box.isConnected || (recover ? reconcile.disabled : publish.disabled)) return;
      const libraryId = recover ? record.libraryId : select.value;
      const choice = recover ? record.ordinals.slice() : selected();
      if (!recover && (previewLibrary !== libraryId || !choice.length || choice.some(ordinal => !eligible.includes(ordinal)))) return;
      const name = libraries.find(library => library.id === libraryId)?.name || 'the recorded library';
      if (!window.confirm(recover ? 'Verify the existing copied files and finish their recorded import into ' + name + '?' : 'Add ' + choice.length + ' selected file(s) to ' + name + '? Files start without a matched title.')) return;
      const intent = action(card), restore = paused();
      knownEmpty = false; state.textContent = recover ? 'Verifying the recorded import…' : 'Adding the selected saved files…';
      try {
        const value = publication(await request(base + (recover ? '/reconcile' : ''), 'POST', stamp, {libraryId, ordinals: choice}));
        if (value.libraryId !== libraryId || JSON.stringify(value.ordinals) !== JSON.stringify(choice) || value.state !== 'registered') throw Error('shape');
        if (owns(card, stamp, intent)) showRecord(value);
      } catch (_) {
        if (owns(card, stamp, intent)) { record = null; resetPreview(); knownEmpty = false; state.textContent = 'The import could not be confirmed and may have completed. Use Check library import before taking further action.'; }
      } finally { if (owns(card, stamp, intent)) restore(); }
    }
    publish.addEventListener('click', () => mutate(false));
    reconcile.addEventListener('click', () => mutate(true));
  }
  function inventory(value, id) {
    if (!keys(value, ['queueId', 'state', 'observedAt', 'entryCount', 'observedBytes', 'files']) || value.queueId !== id || value.state !== 'local_inventory' || !copyTime(value.observedAt) || !Number.isSafeInteger(value.entryCount) || value.entryCount < 1 || value.entryCount > 128 || !Number.isSafeInteger(value.observedBytes) || value.observedBytes < 0 || value.observedBytes > 1099511627776 || !Array.isArray(value.files) || value.files.length !== value.entryCount) throw Error('shape');
    const kinds = {mkv: 'media_candidate', mp4: 'media_candidate', ts: 'media_candidate', m2ts: 'media_candidate', avi: 'media_candidate', nfo: 'other_regular', srt: 'other_regular', other: 'other_regular'};
    let total = 0;
    value.files.forEach((file, i) => {
      if (!keys(file, ['ordinal', 'kind', 'extension', 'observedBytes']) || file.ordinal !== i + 1 || !Object.hasOwn(kinds, file.extension) || file.kind !== kinds[file.extension] || !Number.isSafeInteger(file.observedBytes) || file.observedBytes < 0 || file.observedBytes > 1099511627776 - total) throw Error('shape');
      total += file.observedBytes;
    });
    if (total !== value.observedBytes) throw Error('shape');
    return value;
  }
  function previewControls(item, card, stamp, refresh) {
    const inspect = text('button', 'Inspect local files'); inspect.type = 'button'; inspect.className = 'btn btn-secondary'; inspect.dataset.sabImportPreview = 'true';
    const result = document.createElement('div'); result.setAttribute('role', 'status'); result.setAttribute('aria-live', 'polite');
    const url = new URL(endpoint.href.replace(/\/$/, '') + '/' + item.queueId + '/import-preview');
    inspect.addEventListener('click', async () => {
      if (inspect.disabled || refresh.disabled || !isCurrent(stamp) || !card.isConnected) return;
      const intent = action(card);
      const buttons = Array.from(card.querySelectorAll('button,input,select'), button => [button, button.disabled]);
      for (const [button] of buttons) button.disabled = true;
      result.replaceChildren(text('p', 'Inspecting file details…'));
      try {
        const value = inventory(await request(url, 'POST', stamp), item.queueId);
        if (!owns(card, stamp, intent)) return;
        result.replaceChildren(text('p', value.entryCount + ' regular file(s) observed; ' + value.observedBytes.toLocaleString() + ' bytes.'));
        result.append(text('p', 'Checked: ' + new Date(value.observedAt).toLocaleString()));
        const files = document.createElement('ul');
        for (const file of value.files) {
          const kind = file.kind === 'media_candidate' ? 'Possible media' : 'Other file';
          files.append(text('li', 'File ' + file.ordinal + ': ' + kind + ' (' + file.extension.toUpperCase() + '), ' + file.observedBytes.toLocaleString() + ' bytes.'));
        }
        result.append(files, text('p', 'File details only. No files were copied or added to a library; playback is not confirmed.'));
      } catch (_) {
        if (owns(card, stamp, intent)) result.replaceChildren(text('p', 'File details could not be inspected. No copy was requested.'));
      } finally {
        if (owns(card, stamp, intent)) for (const [button, disabled] of buttons) button.disabled = disabled;
      }
    });
    card.append(inspect, result);
  }
  async function load(target, targetHistory) {
    if (retired || busy) return;
    cancel(); const stamp = ++generation; busy = true; nextCursor = ''; controls();
    rows.replaceChildren(); message.textContent = 'Loading saved observations…';
    try {
      const url = new URL(endpoint); url.searchParams.set('limit', '50'); if (target) url.searchParams.set('cursor', target);
      const page = await request(url, 'GET', stamp);
      if (!keys(page, ['items', 'nextCursor']) || !Array.isArray(page.items) || page.items.length > 50 || page.nextCursor !== undefined && (typeof page.nextCursor !== 'string' || !/^v1\.[1-9][0-9]{0,18}\.[1-9][0-9]{0,18}$/.test(page.nextCursor))) throw Error('shape');
      let prior = Infinity;
      const items = page.items.map(item => { observation(item); if (item.queueId >= prior) throw Error('shape'); prior = item.queueId; return item; });
      if (!isCurrent(stamp)) return;
      cursor = target; history = targetHistory; nextCursor = page.nextCursor || '';
      for (const item of items) { const card = document.createElement('article'); draw(item, card, stamp); rows.append(card); }
      message.textContent = items.length ? 'Saved observations. No provider check was started.' : 'No saved handoffs on this page.';
    } catch (_) {
      if (isCurrent(stamp)) message.textContent = 'Saved observations could not be loaded. Reload saved status to try again.';
    } finally { if (isCurrent(stamp)) { busy = false; controls(); } }
  }
  reload.addEventListener('click', () => load('', []));
  previous.addEventListener('click', () => { const copy = history.slice(); const target = copy.pop(); if (target !== undefined) load(target, copy); });
  next.addEventListener('click', () => { if (nextCursor) load(nextCursor, history.concat(cursor)); });
  window.addEventListener('pagehide', () => { retired = true; generation++; cancel(); controls(); });
  load('', []);
}());
