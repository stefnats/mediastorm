// Component tests execute unmodified functions extracted from the owned Web player.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const player = fs.readFileSync(path.join(__dirname, '../web_templates/playback.html'), 'utf8');
const report = fs.readFileSync(path.join(__dirname, '../static/release-reports-v2.js'), 'utf8');

function actualFunction(source, name, indent = '    ') {
  const start = new RegExp('^' + indent + '(?:async )?function ' + name + '\\(', 'm').exec(source);
  assert.ok(start, name + ' must exist in the production source');
  const after = source.slice(start.index + start[0].length);
  const next = new RegExp('^' + indent + '(?:async )?function [A-Za-z0-9_]+\\(', 'm').exec(after);
  const end = next ? start.index + start[0].length + next.index : source.indexOf('\n  </script>', start.index);
  assert.ok(end > start.index, name + ' must have a closing function boundary');
  return source.slice(start.index, end);
}
const facts = (resolution = '720p') => ({ resolution, resolutionSource:'file_probe', advertisedResolution:'2160p',
  codec:'h264', codecSource:'file_probe', advertisedCodec:'HEVC', audio:'aac', audioSource:'file_probe', audioStreamIndex:'3', advertisedAudio:'TrueHD Atmos',
  hdr:'HDR10', hdrSource:'file_probe', advertisedHDR:'DV', hasDV:'false', advertisedHasDV:'true' });
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return {promise, resolve}; }

function fixture(options = {}) {
  const nodes = new Map(), calls = [], starts = [];
  const element = id => {
    if (!nodes.has(id)) nodes.set(id, {innerHTML:'', textContent:'', hidden:false, disabled:false, value:'rewind', close() { this.closed = true; }});
    return nodes.get(id);
  };
  const context = vm.createContext({
    document:{getElementById:element}, console:{warn() {}, info() {}}, URL, URLSearchParams, Set, Map, Number, clearInterval,
    SHARE_MODE:false, WATCH_PARTY_MODE:false, activeProfileId:'profile', activeProfileName:'Synthetic profile',
    sourcePath:'/old.mkv', directMovieUrl:'', playbackMeta:{title:'Synthetic title', displayName:'Synthetic.2160p', isLive:false},
    webPlayerActivePrequeueId:'old-prequeue', webPlayerPreferences:{streamMigrationEnabled:false}, webPlayerMigrationState:null,
    webSourceQualityRevision:0,
    webPlayerMigrationInProgress:false, webPlayerSubtitleSearchGeneration:0, webPlayerSeekGeneration:0,
    webPlayerSelectedAudioTrack:8, webPlayerSelectedSubtitleTrack:-1, webPlayerManualAudioSelection:true, webPlayerManualSubtitleSelection:false,
    webPlayerExternalSubtitle:null, webPlayerSubtitleCues:[], nextEpisodeInfo:{seasonNumber:1, episodeNumber:2, name:'Next'},
    nextEpisodePrequeue:null, webPlayerMetadataResolved:false,
    currentAbsoluteTime:() => 42, setStatus() {}, webPlayerDebugLog() {}, sendWebPlayerProgress() {},
    clearWebMigrationStallTimer() {}, loadWebMigrationBadStreams:async () => [], webMigrationCandidateSessionKey:candidate => candidate.guid,
    webMigrationCandidateMatchesBadStream:() => false, webMigrationPathIdentity:value => value, startWebMigrationProbation() {},
    syncSubtitleOffset() {}, resetNextEpisodeState() {}, loadEffectiveWebPlaybackSettings:async () => {}, syncPlaybackUrlToHistory() {},
    ensureNextEpisodeInfo:async () => null, releaseReportLanguageName:code => code || 'Unknown language',
    releaseReportBusy:false, releaseReportGeneration:0, releaseReportAction:null, releaseReportState:null,
    releaseActionCurrent:() => true, releaseReportWait:promise => promise, cancelReleaseAction() {}, stopHlsSession() {},
    apiCall:async (url, init = {}) => { calls.push({url, ...init}); return options.api ? options.api(url, init) : {}; },
  });
  context.setReleaseReportBusy = value => { context.releaseReportBusy = value; };
  context.startWebPlayerFromSource = async value => { starts.push({path:context.sourcePath, quality:context.playbackMeta.sourceQuality, ...value}); };
  context.waitForWebPlayerUsable = async () => { if (options.failUsable) throw new Error('Synthetic unusable replacement'); };
  for (const name of ['escapeHtml', 'webSourceQualityAttributes', 'webSourceQualityRows', 'renderWebQualityComparison', 'renderWebSourceQuality',
    'setWebSourceQuality', 'clearWebSourceQuality', 'renderHeader', 'startManualPath', 'initializeWebMigrationState', 'applyWebMigrationMetadata', 'migrateWebStream', 'switchToNextEpisode']) {
    vm.runInContext(actualFunction(player, name), context);
  }
  for (const name of ['renderPreparedRelease', 'preparedReleaseOffset', 'startPreparedRelease']) vm.runInContext(actualFunction(report, name, ''), context);
  return {context, element, calls, starts};
}

test('file facts use their own markers and retain separate original claims', () => {
  const f = fixture(), attributes = facts(); delete attributes.resolutionSource;
  const rows = f.context.webSourceQualityRows(attributes);
  assert.ok(rows.some(([label, value]) => label === 'File video codec' && value === 'H.264'));
  assert.ok(rows.some(([label, value]) => label === 'Advertised video codec' && value === 'HEVC'));
  assert.ok(rows.some(([label, value]) => label === 'File audio (prepared track)' && value === 'AAC · container stream 3'));
  assert.ok(rows.some(([label, value]) => label === 'Advertised audio' && value === 'TrueHD Atmos'));
  assert.ok(rows.some(([label, value]) => label === 'File dynamic range' && value === 'HDR10'));
  assert.ok(rows.some(([label, value]) => label === 'File Dolby Vision' && value === 'No'));
  assert.equal(rows.some(([label]) => label === 'File resolution'), false);
  assert.equal(attributes.advertisedHDR, 'DV');
});

test('unverified codec HDR and aggregate audio never become observed file facts or SDR', () => {
  const f = fixture();
  assert.equal(f.context.webSourceQualityRows({codec:'HEVC', hdr:'DV', hasDV:'false'}).length, 0);
  assert.equal(f.context.webSourceQualityRows({audio:'aac', audioSource:'file_probe', advertisedAudio:'Atmos'}).length, 0);
  assert.equal(f.context.webSourceQualityRows({hdrSource:'file_probe', hdr:'unknown', hasDV:'false'}).length, 0);
  f.context.setWebSourceQuality('/old.mkv', {});
  assert.match(f.element('sourceQuality').innerHTML, /File quality not verified/);
  assert.doesNotMatch(f.element('sourceQuality').innerHTML, /SDR/);
});

test('current and prepared comparison render separately and escape provider text', () => {
  const f = fixture(); f.context.setWebSourceQuality('/old.mkv', facts());
  const ready = {streamPath:'/prepared.mkv', resultAttributes:{...facts('1080p'), advertisedCodec:'<b>provider claim</b>'}, selectedAudioTrack:3, audioTracks:[]};
  f.context.renderPreparedRelease({ready, reason:'quality_problem'});
  assert.match(f.element('releaseReplacementQuality').innerHTML, /1080p/);
  assert.match(f.element('releaseReplacementQuality').innerHTML, /&lt;b&gt;provider claim&lt;\/b&gt;/);
  assert.doesNotMatch(f.element('releaseReplacementQuality').innerHTML, /<b>provider claim/);
  assert.equal(f.context.playbackMeta.sourceQuality.attributes.resolution, '720p');
  assert.match(f.element('sourceQuality').innerHTML, /720p/);
});

test('existing initial status read supplies quality when migration is off without polling or probes', async () => {
  const f = fixture({api:async () => ({streamPath:'/old.mkv', resultAttributes:facts(), migrationCandidates:[]})});
  await f.context.initializeWebMigrationState('old-prequeue');
  assert.deepEqual(f.calls.map(call => call.url), ['/playback/prequeue/old-prequeue']);
  assert.equal(f.context.webPlayerMigrationState, null);
  assert.equal(f.context.playbackMeta.sourceQuality.attributes.resolution, '720p');
});

test('manual path clears prior facts and a delayed old status cannot put them back', async () => {
  for (const manualPath of ['/manual.mkv', '/old.mkv']) {
  const pending = deferred(), f = fixture({api:() => pending.promise});
  f.context.setWebSourceQuality('/old.mkv', facts());
  const old = f.context.initializeWebMigrationState('old-prequeue');
  f.element('manualPath').value = manualPath;
  await f.context.startManualPath();
  pending.resolve({streamPath:'/old.mkv', resultAttributes:facts()}); await old;
  assert.equal(f.context.sourcePath, manualPath);
  assert.equal(f.context.playbackMeta.sourceQuality, null);
  assert.equal(f.context.playbackMeta.displayName, '');
  assert.match(f.element('sourceQuality').innerHTML, /File quality not verified/);
  assert.equal(f.starts[0].path, manualPath);
  }
});

test('next episode and explicit prepared adoption use the new file or clear unknown quality', async () => {
  const f = fixture(); f.context.setWebSourceQuality('/old.mkv', facts());
  await f.context.switchToNextEpisode({prequeueId:'next', streamPath:'/next.mkv', displayName:'Next release', resultAttributes:facts('1080p')});
  assert.equal(f.context.playbackMeta.sourceQuality.path, '/next.mkv');
  assert.equal(f.context.playbackMeta.sourceQuality.attributes.resolution, '1080p');
  f.context.nextEpisodeInfo = {seasonNumber:1, episodeNumber:3};
  await f.context.switchToNextEpisode({prequeueId:'unknown', streamPath:'/unknown.mkv'});
  assert.match(f.element('sourceQuality').innerHTML, /File quality not verified/);
  const ready = {status:'ready', prequeueId:'prepared', streamPath:'/prepared.mkv', resultAttributes:facts('480p'), selectedAudioTrack:3};
  f.context.apiCall = async () => ready;
  f.context.releaseReportAction = {ready, replacementId:'prepared', reason:'quality_problem', generation:0};
  await f.context.startPreparedRelease();
  assert.equal(f.context.playbackMeta.sourceQuality.path, '/prepared.mkv');
  assert.equal(f.context.playbackMeta.sourceQuality.attributes.resolution, '480p');
});

test('actual migration applies corrected resolve attributes and rollback restores previous quality', async () => {
  for (const failed of [false, true]) {
    const candidate = {guid:'candidate', title:'Advertised.2160p', attributes:{resolution:'2160p', codec:'HEVC'}};
    const f = fixture({failUsable:failed, api:async url => url === '/playback/resolve'
      ? {webdavPath:'/replacement.mkv', qualityAttributes:facts('1080p')} : {videoStreams:[{codecName:'h264'}]}});
    f.context.webPlayerPreferences.streamMigrationEnabled = true;
    f.context.webPlayerMigrationState = {prequeueId:'old-prequeue', candidates:[candidate], attempted:new Set(), attemptedKeys:new Set()};
    f.context.setWebSourceQuality('/old.mkv', facts());
    const migrated = await f.context.migrateWebStream('synthetic stall');
    assert.equal(migrated, !failed);
    assert.equal(f.context.sourcePath, failed ? '/old.mkv' : '/replacement.mkv');
    assert.equal(f.context.playbackMeta.sourceQuality.attributes.resolution, failed ? '720p' : '1080p');
    assert.equal(candidate.attributes.resolution, '2160p');
    assert.equal(candidate.attributes.codec, 'HEVC');
    assert.equal(f.calls.filter(call => call.url === '/playback/resolve').length, 1);
    assert.equal(f.calls.filter(call => call.url.startsWith('/video/metadata?')).length, 1);
  }
});
