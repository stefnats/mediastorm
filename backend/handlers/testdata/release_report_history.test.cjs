const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const reportSource = fs.readFileSync(path.join(__dirname, '../static/release-reports-v2.js'), 'utf8');
const playerSource = fs.readFileSync(path.join(__dirname, '../web_templates/playback.html'), 'utf8');
function section(start, end) {
  const from = playerSource.indexOf('    function ' + start);
  const to = playerSource.indexOf('    function ' + end, from + 1);
  assert.ok(from >= 0 && to > from, 'actual player function section must exist');
  return playerSource.slice(from, to);
}
function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
const tick = () => new Promise(resolve => setImmediate(resolve));
function fixture(options = {}) {
  const nodes = new Map(), calls = [];
  let pauses = 0;
  function element(id) {
    if (!nodes.has(id)) nodes.set(id, { hidden: true, checked: false, disabled: false, value: '', textContent: '', open: false,
      showModal() { this.open = true; }, close() { this.open = false; }, focus() {}, addEventListener() {},
      replaceChildren() {}, add(option) { this.value = option.value; }, pause() { pauses++; } });
    return nodes.get(id);
  }
  async function apiCall(url, init = {}) {
    calls.push({url, ...init});
    if (url.endsWith('/history/progress')) return options.progress ? options.progress.promise : {itemId: 'tmdb:movie:123', percentWatched: 20};
    if (init.method === 'DELETE') return {};
    if (init.method === 'POST') {
      const body = JSON.parse(init.body);
      return {report: {id: 'saved-report'}, ...(body.resetHistory ? (options.resetFailed ? {historyReset: false, historyResetError: 'history_reset_failed'} : {historyReset: true}) : {})};
    }
    return {sourceVersion: 'selected-source', releaseName: 'Synthetic release', canResetHistory: true, canShare: false, hasEpisode: true, audioLanguage: 'fra', audioTracks: [], selectedAudioTrack: -1};
  }
  const context = vm.createContext({document: {getElementById: element, querySelector: element, documentElement: {lang: 'en'}, addEventListener() {}},
    window: {addEventListener() {}}, navigator: {language: 'en'}, Intl, Option: class {}, setTimeout, clearTimeout,
    setInterval: () => 1, clearInterval() {}, Date, console, apiCall, SHARE_MODE: false, WATCH_PARTY_MODE: false, CLIENT_ID: 'device',
    activeProfileId: 'profile', sourcePath: '/old.mkv', webPlayerActivePrequeueId: 'old-prequeue',
    playbackMeta: {isLive: false}, webPlayerAudioPreferenceGeneration: 0, webPlayerAudioPreferenceSave: Promise.resolve(), webPlayerAudioPreferenceSaveFailed: false,
    webPlayerMigrationInProgress: false, webPlayerMigrationProbation: false, setStatus() {},
    currentAbsoluteTime: () => 120, currentDuration: () => 600, progressPositionForSend: () => 120,
    rememberPositivePosition() {}, buildProgressPayload: () => ({mediaType: 'movie', itemId: 'tmdb:movie:123', position: 120, duration: 600}),
    webPostPlayKey: () => 'current', webPlayerDebugLog() {}, discardWebPostPlaySource() {},
    apiUrl: url => url, apiAuthHeaders: x => x, fetch: async () => ({}),
    WEB_PLAYER_PROGRESS_INTERVAL_MS: 10000, WEB_PLAYER_PROGRESS_MIN_DELTA_SECONDS: 10,
  });
  vm.runInContext(`const webProgressDiscardedSources = new Set(); const webProgressRequests = new Map(); let progressTimer = 1;
    let progressCompleted = false, webPlayerBuffering = false, lastProgressSentAt = 0, lastProgressPosition = 0, lastProgressPaused = false, lastProgressBuffering = false, webPlayerCanonicalProgressItemId = null;`, context);
  vm.runInContext(section('webProgressSourceContext', 'startProgressTracking'), context);
  vm.runInContext(section('stopProgressTracking', 'sendSharePlaybackProgress'), context);
  vm.runInContext(section('sendCapturedWebProgress', 'parseWebVttCues'), context);
  vm.runInContext(section('sendWebPlayerProgress', 'buildProgressPayload'), context);
  vm.runInContext(reportSource, context);
  return {context, calls, element, pauses: () => pauses, async open(reason) {
    await context.openReleaseReport(); element('releaseReportReason').value = reason; context.updateReleaseReportScope();
  }};
}

test('history reset is opt-in and an ordinary report keeps normal progress', async () => {
  const f = fixture(); await f.open('wrong_title');
  assert.equal(f.element('releaseReportHistoryRow').hidden, false);
  assert.equal(f.element('releaseReportResetHistory').checked, false);
  await f.context.submitReleaseReport(false);
  const body = JSON.parse(f.calls.find(call => call.method === 'POST').body);
  assert.equal(body.resetHistory, undefined);
  assert.equal(f.pauses(), 0);
  await f.context.sendWebPlayerProgress({force: true});
  assert.equal(f.calls.filter(call => call.url.endsWith('/history/progress')).length, 1);
});

test('reset waits for an actual pending player write and blocks every later old-source write', async () => {
  const pending = deferred(), f = fixture({progress: pending});
  await f.open('wrong_episode');
  const first = f.context.sendWebPlayerProgress({force: true});
  f.element('releaseReportResetHistory').checked = true;
  const submitting = f.context.submitReleaseReport(false);
  await tick();
  assert.equal(f.pauses(), 1);
  assert.equal(f.calls.filter(call => call.url.endsWith('/report') && call.method === 'POST').length, 0, 'reset must wait for the earlier stored progress');
  await f.context.sendWebPlayerProgress({force: true, ended: true});
  await f.context.sendWebPlayerProgress({force: true, keepalive: true});
  await f.context.sendCapturedWebProgress({itemId: 'tmdb:movie:123'});
  assert.equal(f.calls.filter(call => call.url.endsWith('/history/progress')).length, 1);
  pending.resolve({percentWatched: 20}); await first; await submitting;
  const body = JSON.parse(f.calls.find(call => call.url.endsWith('/report') && call.method === 'POST').body);
  assert.equal(body.resetHistory, true);
  assert.equal(body.reason, 'wrong_episode');
  assert.match(f.element('releaseReportError').textContent, /saved position is cleared/);
  await f.context.sendWebPlayerProgress({force: true, ended: true});
  assert.equal(f.calls.filter(call => call.url.endsWith('/history/progress')).length, 1);
  f.context.sourcePath = '/replacement.mkv'; f.context.webPlayerActivePrequeueId = 'replacement';
  await f.context.sendWebPlayerProgress({force: true});
  assert.equal(f.calls.filter(call => call.url.endsWith('/history/progress')).length, 2, 'a fresh replacement must still save its progress');
  await f.context.undoReleaseReport();
  assert.match(f.element('releaseReportError').textContent, /history reset is not undone/);
});

test('non-content reasons clear the opt-in and never request history reset', async () => {
  const f = fixture(); await f.open('wrong_title'); f.element('releaseReportResetHistory').checked = true;
  f.element('releaseReportReason').value = 'quality_problem'; f.context.updateReleaseReportScope();
  assert.equal(f.element('releaseReportHistoryRow').hidden, true);
  assert.equal(f.element('releaseReportResetHistory').checked, false);
  await f.context.submitReleaseReport(false);
  assert.equal(JSON.parse(f.calls.find(call => call.method === 'POST').body).resetHistory, undefined);
  assert.equal(f.pauses(), 0);
});

test('a saved report with an unconfirmed reset is explicit and starts no alternative', async () => {
  const f = fixture({resetFailed: true}); await f.open('wrong_title'); f.element('releaseReportResetHistory').checked = true;
  await f.context.submitReleaseReport(true);
  assert.match(f.element('releaseReportError').textContent, /Report saved, but the history reset could not be confirmed/);
  assert.equal(f.calls.filter(call => !call.url.endsWith('/report') && call.method !== 'DELETE').length, 0);
  assert.equal(f.element('releaseReplacementReady').hidden, true);
});
