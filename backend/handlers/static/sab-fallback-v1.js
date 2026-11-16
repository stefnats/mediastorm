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
      if (!reply.ok) throw Error('request');
      if (!(reply.headers.get('content-type') || '').includes('application/json')) throw Error('shape');
      const value = await reply.json();
      if (!isCurrent(stamp)) throw Error('stale');
      return value;
    } finally { pending.delete(controller); }
  }
  function text(tag, value) { const node = document.createElement(tag); node.textContent = value; return node; }
  function draw(item, card, stamp) {
    card.replaceChildren(text('h3', 'Queue ' + item.queueId), text('p', states[item.state]));
    if (item.reason) card.append(text('p', reasons[item.reason]));
    for (const [key, label] of [['submittedAt', 'Handoff observed'], ['checkedAt', 'Status checked']]) {
      card.append(text('p', label + ': ' + (item[key] ? new Date(item[key]).toLocaleString() : 'Not recorded')));
    }
    const refresh = text('button', 'Refresh status'); refresh.type = 'button'; refresh.className = 'btn btn-secondary';
    const status = text('p', ''); status.setAttribute('role', 'status');
    refresh.addEventListener('click', async () => {
      if (!isCurrent(stamp) || refresh.disabled) return;
      refresh.disabled = true; status.textContent = 'Checking this existing handoff…';
      try {
        const value = observation(await request(endpoint.href + '/' + item.queueId + '/refresh', 'POST', stamp));
        if (value.queueId !== item.queueId) throw Error('shape');
        if (isCurrent(stamp) && card.isConnected) draw(value, card, stamp);
      } catch (_) {
        if (isCurrent(stamp) && card.isConnected) status.textContent = 'Status could not be refreshed. Saved observation is unchanged; try Refresh status again.';
      } finally { if (isCurrent(stamp) && card.isConnected) refresh.disabled = false; }
    });
    card.append(refresh, status);
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
