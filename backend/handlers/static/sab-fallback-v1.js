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
  async function request(url, method, stamp) {
    const controller = new AbortController(); pending.add(controller);
    try {
      const reply = await fetch(url, {method, credentials: 'same-origin', cache: 'no-store', redirect: 'error', headers: {Accept: 'application/json'}, signal: controller.signal});
      if (!isCurrent(stamp)) throw Error('stale');
      if (reply.status === 401 || reply.status === 403) { terminal(); throw Error('terminal'); }
      if (!reply.ok) {
        const error = Error('request');
        try {
          const body = await reply.json();
          if (body && ['copy_already_recorded', 'copy_budget_exceeded', 'copy_outcome_unknown', 'fallback_busy'].includes(body.code)) error.code = body.code;
        } catch (_) { /* Errors remain fixed local messages. */ }
        throw error;
      }
      if (!(reply.headers.get('content-type') || '').includes('application/json')) throw Error('shape');
      const value = await reply.json();
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
      for (const button of card.querySelectorAll('button')) button.disabled = true;
      status.textContent = 'Checking this existing handoff…';
      try {
        const value = observation(await request(endpoint.href + '/' + item.queueId + '/refresh', 'POST', stamp));
        if (value.queueId !== item.queueId) throw Error('shape');
        if (owns(card, stamp, intent)) draw(value, card, stamp);
      } catch (_) {
        if (owns(card, stamp, intent)) status.textContent = 'Status could not be refreshed. Saved observation is unchanged; try Refresh status again.';
      } finally { if (owns(card, stamp, intent)) for (const button of card.querySelectorAll('button')) button.disabled = false; }
    });
    card.append(refresh, status);
    if (panel.dataset.managedCopy === 'true') copyControls(item, card, stamp, refresh);
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
    function controls(waiting) { view.disabled = waiting; refresh.disabled = waiting; copy.disabled = waiting || !eligible(); }
    function show(value) {
      current = value;
      const labels = {unknown: 'Saved copy status is unknown.', not_recorded: 'No saved copy is recorded for this location.', copy_recorded: 'A saved copy was recorded. This does not mean ready to play.', reconciliation_required: 'This copy needs checking. It cannot be copied again here.'};
      state.textContent = value.reason === 'not_configured' ? 'Copy files is not set up.' : value.reason === 'configuration_changed' ? 'The download settings have changed. Saved copy status is unknown.' : labels[value.state];
      if (value.chargedBytes !== undefined) state.append(text('span', ' Counted toward the storage limit: ' + value.chargedBytes.toLocaleString() + ' bytes.'));
      hideCopy(!eligible()); controls(false);
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
