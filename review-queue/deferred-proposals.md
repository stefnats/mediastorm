# Deferred proposals

PRs [81](https://github.com/godver3/mediastorm/pull/81), [82](https://github.com/godver3/mediastorm/pull/82) and [96](https://github.com/godver3/mediastorm/pull/96) were merged by the maintainer on 15 November 2026 at 19:30 Europe/Berlin. At that check, no own upstream PR was open in either repository. A later independently created draft [103](https://github.com/godver3/mediastorm/pull/103), opened on 16 November at 00:07 Europe/Berlin, concerns opt-in NZB download policy and remains untouched by this work. Deferred features remain in the forks pending scope discussion. Successive fork-only entries below retain the review status at the time of each package.

These are candidate review boundaries for discussion. All listed feature PRs are closed; their source remains in the forks. Their original histories contain stacked changes, so neither an old PR diff nor a group below is ready to be resubmitted as a single review unit. No new isolation, extraction or test run is claimed here.

| Topic | Candidate first review unit | Follow-up scope to agree | Historical source |
| --- | --- | --- | --- |
| IPTV capacity and sender alternatives | Shared source-slot accounting across live playback and recording | Station grouping/failover and recording stop causes need separate scopes; takeover rights and recording confirmation are tracked as fork-only work below | Backend 83, 84, 93; native 18 |
| Local playback and release recovery | Automatic local-file fallback with a profile preference | Language-scoped reports, prepared replacements and client recovery prompts need separate API/client boundaries | Backend 85, 86, 87; native 4, 5 |
| Source availability | Cached, profile-aware title source hints with a bounded search budget | Report invalidation, episode checks, viewport demand, local labels and manual rechecks are separate follow-ups | Backend 79, 88, 89, 90, 91, 92; native 6, 7, 8, 9, 10 |
| Recommendations and taste | Optional profile-specific interest signals | Ratings and taste setup should each define their storage/API behavior before their client UI is proposed | Backend 80, 98, 99; native 11, 12 |
| Personal lists and client context | Profile/account isolation of in-flight client requests and list responses | The personal-list entry point is a separate UI change; the two isolation topics need independent extraction checks | Backend 100; native 13, 14, 17 |
| Episode autoplay | A profile-level setting independent of credit markers | Agree the backend setting and matching native control together, with an explicit compatibility plan | Backend 101; native 15 |
| Completion choices | Optional rating and next-title controls after actual playback completion | Define its dependence on the rating and recommendation contracts before extracting the UI | Backend 102; native 16 |

The combined backend proposal 97 is retained only as historical integration evidence. It is closed and is not a candidate review unit.

## Submission process

Agree one topic with the maintainer before opening its next PR. Extract that change onto the current upstream and remove unrelated commits, generated review artifacts and earlier features. If a prerequisite remains, name its PR and the required merge order rather than silently carrying it in the diff.

Run the tests needed for the extracted branch and record its exact head. Describe the trigger, resulting behavior, entry point for review and remaining limits. Backend/client changes must link their matching contract and deployment order. Keep unrelated proposals in the fork while that review proceeds; do not open the whole table at once.

[Source archive](source-archive.md) maps every historical PR to its preserved head and fork branch. The proposed boundaries above are not evidence that those features already work independently.

## Fork-only IPTV follow-up

[Backend implementation](https://github.com/stefnats/mediastorm/tree/6ade63a203d01c2a80cf4473fb53022aa30ff103) and [TV/phone controls](https://github.com/stefnats/mediastorm-androidtv/tree/6a6a72a5c961566b66e6ae68993655e80786c702) add default-off, account-managed live takeover and separate recording-interruption rights. Recording interruption requires an expiring confirmation; partial output remains saved. Revoked playback and marked automatic retunes cannot displace another viewer. The normal TV and phone Play flow presents confirmation; older clients, casting and automatic multiscreen starts remain non-preempting.

The combined backend fixture suite passed 8,267 cases with 37 explicit skips; repeated race checks cover the changed services and HTTP paths. The final API-contract follow-up passed its repeated checks separately. All 895 native tests pass, and both debug APKs build. Cookie-authenticated account templates were checked at six widths using synthetic profiles. These branches retain the preserved integration history and are not isolated upstream PR candidates. Physical-device acceptance, automatic provider-limit discovery and planning conflicts remain open. No application was reactivated or deployed.

## Fork-only provider-limit inspection

[Backend implementation](https://github.com/stefnats/mediastorm/tree/0cddc17deca96db89f9ac49a72768cfb6660f72c) adds an administrator-only check for a saved Xtream source or its recognized M3U export. It requests bounded account metadata without opening a stream, blocks redirects, honors the configured proxy and coalesces repeated checks. Provider messages and credentials are not returned. A missing or zero reported limit remains unknown.

The settings dialog displays origin, freshness, the provider connection snapshot, local reservations and configured shared caps separately. Using a reported limit only edits the form; the existing explicit save applies it. Source changes and expired reports prevent adoption. There is no background discovery or automatic runtime limit change, and the snapshot does not identify which connections belong to external applications.

The complete backend suite passed 8,306 cases with 38 explicit skips. Three repeated race runs passed 120 provider-capacity cases; the browser fixture was explicitly skipped in those race-only runs. The real cookie-authenticated settings template was checked at six widths from 320 to 1280 pixels, including failure, cancellation, stale source, retry and explicit save paths. API contract checks and the backend build pass. All provider responses are synthetic. Native code was unchanged and its previous results were not rerun for this package.

This branch retains the earlier integration history and is not an isolated upstream submission. Runtime provider-limit adoption, dependable external-usage attribution and predictive recording-conflict planning remain open. The active upstream queue remains PRs 81, 82 and 96. No new upstream PR, comment, deployment or application reactivation was made.

## Fork-only recording-conflict forecast

[Backend and web implementation](https://github.com/stefnats/mediastorm/tree/2e9ae165d9ba95c5254e2f49b8b8aebfb94445e9) adds read-only forecasts for already materialized recording jobs and proposed EPG/manual recordings. Padded half-open windows count each private recorder input against shared provider-account caps, including stricter aliases. Aggregate demand includes other accounts, while returned job IDs remain scoped to the requested owned adult profile. Unconfigured limits, truncated scans and unresolved overlapping sources cannot produce a clear forecast.

The recording page displays upcoming conflicts and checks a new job before scheduling it. A conflict or unavailable forecast asks for an explicit choice; cancellation and Escape preserve the schedule. Rechecking never creates a job, and form/profile changes or late responses invalidate pending choices. The forecast never reserves a connection or changes an existing job. Actual atomic admission remains final: automatic recordings wait for capacity without displacing viewers. No priority is granted by the forecast.

The complete backend suite passed 8,354 cases with 38 explicit skips. Three repeated race runs passed 144 planning, access and child-policy cases; the browser test was explicitly skipped in those race-only runs. Real cookie-authenticated recording templates and handlers were checked at six widths from 320 to 1280 pixels, including confirmed single creation, double clicks, retry, cancellation, profile changes, delayed responses, expiry, unavailable/rejected forecasts and the EPG path. API contract checks passed 32 cases and the final backend build passes. Provider inputs and jobs are synthetic. Native UI and database migrations are unchanged and were not independently rebuilt or exercised for this package.

The forecast covers existing materialized jobs, not unexpanded rule matches, future live viewers or external applications. It is advisory and does not promise a slot. Native forecast controls, external-usage attribution and runtime provider-limit adoption remain open. This branch retains the earlier integration history and is not an isolated upstream submission. The upstream queue remains PRs 81, 82 and 96 with unchanged heads. No new upstream PR, comment, deployment or application reactivation was made.

## Fork-only native recording forecasts

[TV and phone implementation](https://github.com/stefnats/mediastorm-androidtv/tree/781abf4cc5c992dfdbc05d2650192f38e4af353b) uses the [matching backend forecast](https://github.com/stefnats/mediastorm/tree/2e9ae165d9ba95c5254e2f49b8b8aebfb94445e9) before creating an EPG recording or a two-hour block. Preview and creation retain the same immutable time window. Conflicts or unknown capacity require Cancel, Check again or an explicit Schedule anyway choice. The TV prompt keeps Cancel first; the phone prompt has scrollable text. Expired warnings require a new check. Upcoming recording details and phone rows show forecast hints.

Consent belongs to the original profile/account/server epoch and owned screen. Context changes, screen disposal, source/filter changes, stale prompts, duplicate taps and late success or error replies cannot reuse it for another intent. Authentication and input refusals are not overridable. Servers without the preview route reject new single recordings with a compatibility message; optional list forecasts remain unavailable. Series-rule creation is unchanged and unexpanded future airings are not forecast.

Creation disables blind replay after uncertain transport failures, remote recovery and redirects. Existing authentication/PIN handling remains scoped to its request identity. Forecasts are advisory, reserve no capacity and grant no right to interrupt another viewer or recording.

All 921 native unit tests pass: 788 core, 44 TV and 89 phone, including 26 new scheduling and transport checks. Two loopback HTTP tests verify a dropped reply and a 307 response without replay. Three schema checks validate the native preview fixture and response shape against the matching backend OpenAPI validator. Both debug APKs build and all five private debug prefills are empty. Test data are synthetic. These are unit, transport, schema and build results; the dialogs were not rendered on an emulator or physical device, and no full client-to-backend login was exercised.

The branch retains the preserved integration history and is not an isolated upstream submission. Native focus, touch, small-screen and physical-device acceptance remain open, as do unexpanded rule conflicts, dependable external-usage attribution and runtime provider-limit adoption. The independent upstream fixes 81, 82 and 96 have now merged; no own upstream PR remains open. No new upstream PR, comment, deployment, reactivation or device installation was made for this package.

## Fork-only unexpanded recording-rule forecast

[Backend and web implementation](https://github.com/stefnats/mediastorm/tree/fbfbdd82d848e7dddde4cebb854c04f0a7726c79) includes known cached-guide matches before enabled recording rules materialize jobs. Projected airings are read-only, own-profile items with no recording ID or job actions. Shared provider demand includes other accounts without disclosing their rule or channel metadata. The web list and scheduling warning display the forecast horizon and coverage status. Existing clients can ignore the additive metadata while using the corrected candidate state.

The forecast retains title/regex, new-only, offset and padding behavior. It reconciles active and terminal history to avoid duplicate projected demand or reviving manually cancelled jobs. Missing guide coverage, invalid data and processing bounds remain uncertain; known conflicts can still be reported. Internal catalogs resolve under each rule owner's identity while the public channel endpoint still refuses foreign profiles.

The horizon is seven days, with at most 200 enabled rules processed, 500 catalog channels per profile, 20,000 cumulative per-rule program inspections and 2,000 projections. Existing pending jobs retain their stored windows until actual expansion updates them. Live viewers, external applications and guide changes are not guaranteed. No job, input, reservation or running scheduler behavior is changed.

The complete backend suite passed 8,407 cases with 38 explicit skips. Three service race runs passed 216 cases and three HTTP race runs passed nine cases. API schema checks passed, the backend builds, and the real cookie-authenticated web page was checked at six widths, including profile switching, no projection job actions, cancellation, incomplete coverage, escaped titles, old responses and unavailable forecasts. UI edge-case response mutations are fixtures, not real provider behavior. The new internal-catalog regression uses only a loopback M3U server. No private playlist, provider/indexer access, native rebuild or device acceptance was used for this package.

This branch retains the integration history and is not an isolated upstream PR candidate. Further features remain pending scope discussion. No own upstream PR is open, and none was created for this package. No deployment or reactivation occurred.

## Fork-only scoped local source preferences

Profiles can prefer eligible local files, use normal search, or inherit at title,
canonical season and episode scope. A dedicated capability-gated write preserves
audio and subtitle preferences. Pending preparations retire after source changes;
active playback IDs remain usable for progress and release reports. Migration 074
is required and reversible.

Backend: [feature/content-local-preference](https://github.com/stefnats/mediastorm/tree/feature/content-local-preference), `d77c5e573352a715ecab29d83351753b94ad38a7`.
TV/phone: [feature/content-local-controls](https://github.com/stefnats/mediastorm-androidtv/tree/feature/content-local-controls), `e237329fde94c5da8a7024ffd04cca3ad3abd25c`.

Validation: 8,395 backend tests passed, 38 explicit skips; synthetic PostgreSQL
fresh/upgrade/rollback checks, repeated preparation races, six native request
fixtures against the backend contract, 952 native tests and both debug APKs.
A separate test-only commit synchronizes the existing completion browser fixture.
Native inherited policy still retains the prior library-first behavior including
Plex/Jellyfin. Alternate numbering is title-only. Physical native rendering,
focus and touch remain untested. No deployment or new upstream proposal.

## Fork-only observed partial series coverage

[feature/partial-series-availability](https://github.com/stefnats/mediastorm/tree/feature/partial-series-availability)
adds optional observed episode/season counts without changing legacy scalar states.
Only current found-plus-missing evidence is partial. A general series candidate
never proves complete availability; conflicting episode aliases remain unknown.
Existing cache and local proofs are revalidated against profile, language,
quality, rights, report revisions and expiry without new search/probe work.

Validation: 162 source/report cases passed, one explicit skip; 87 repeated race
cases, the full OpenAPI contract suite and real local HTTP title/episode responses.
Two independent source reviews completed without remaining concrete findings.
Native display is complete in [feature/partial-series-hints](https://github.com/stefnats/mediastorm-androidtv/tree/feature/partial-series-hints): 969 tests and both APKs passed after an independent review found and corrected stale aggregate hints. Twelve native serializer examples match the backend schema. Physical rendering, focus and touch remain untested. No migration, deployment or new upstream proposal.

## Fork-only split kids-rating API routing

[feature/kids-rating-api-routes](https://github.com/stefnats/mediastorm/tree/feature/kids-rating-api-routes)
registers the existing movie and TV cap handlers in the regular profile API.
The existing protected router retains ownership, profile PIN and fresh parental
authorization. The legacy combined route and admin/account web routes are unchanged.

Validation: 62 API cases including 21 real-router regressions, plus 35 existing
parent-authorization cases. Two source files changed; no regional policy enabled.

## Combined development verification

[feature/integrated-requirements](https://github.com/stefnats/mediastorm/tree/feature/integrated-requirements)
retains the separate source packages and combines rule forecasts, scoped local
preferences and partial-series coverage. The combined baseline at
`ab8035022f69fa3832f6b4d1d6046d36ed9a784c` passed 8,477 backend cases with 38
explicit skips, including synthetic PostgreSQL and authenticated browser fixtures.
The separately tested split-rating routing fix is included in the current branch.
This is a development integration branch, not a new upstream proposal or deployment.

## Fork-only regional certification provenance

[feature/regional-certification-foundation](https://github.com/stefnats/mediastorm/tree/feature/regional-certification-foundation)
retains bounded country/release-specific raw ratings from existing TMDB responses,
with actual provider identity and original retrieval/expiry times. Conflicting
release rows remain distinct. Legacy scalar selection and request scheduling stay
compatible. Cached copies do not renew evidence; people/unknown media never
inherit series evidence through a numeric-ID collision. Manual-match and backup
inputs cannot manufacture provider provenance.

This is metadata foundation only. It enables no regional child authorization,
claims no official registry verification and treats series ratings separately
from episode evidence. No migration is required. Scope: 23 files, 17 new test
functions. Validation: 804 affected-package tests, 51 repeated race cases,
43 API/validator cases, full source build and an independent review completed.

The earlier combined integration at
`69cc67ac7d41628be4a2a9eaa8705a65f3c9c5e3` passed 8,515 backend cases with 38
explicit skips, including synthetic PostgreSQL and authenticated loopback browser
fixtures. The committed backend binary has matching VCS metadata. Source packages
remain separate. No production deployment, activation or new upstream proposal.

## Fork-only personal reminders and inbox

[Backend](https://github.com/stefnats/mediastorm/tree/feature/profile-reminders)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-reminders-inbox)
add adult-profile release, episode-metadata and matching-source interests with a
paged inbox. Source interests use the creating device's preferences. Existing
normal observations deliver events; there is no new polling service or external
delivery. An observed candidate is not a playback guarantee. Children are denied.

Migration 075 and the private JSON fallback preserve deduplication, ownership,
history and revocation. Restore retains messages without renewing source proof or
trusting imported catalog aliases. Opening a reminder does not start playback or
preparation; ordinary configured detail-page source hints remain unchanged.

Validation: 3,492 affected backend cases, 270 repeated race cases, synthetic
PostgreSQL migration/backup/concurrency checks, 1,011 native tests and both APK
builds. All 13 native request fixtures pass real local handlers and OpenAPI.
Physical TV/phone rendering, focus and touch remain untested.

The combined development commit
`57fc0816599b720ecbcdeebe35a05e8678bafeed` passes 8,592 backend cases. Its optional
native fixture path was then exercised separately with all 13 examples, leaving
38 pre-existing opt-in skips. The backend binary has matching commit metadata.
Packages remain in separate fork branches; the combined branch is for integration
verification. No new upstream PR, production deployment or service activation.

## Fork-only automatic library selection

[Backend](https://github.com/stefnats/mediastorm/tree/feature/automatic-library-selection)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-automatic-library-selection)
route supported catalog Play, next episodes and prefetch through one automatic
selection policy. Episode/season/title preferences precede profile/server values.
Eligible local, Plex and Jellyfin copies are preferred or used as fallback.
Explicit Versions and Stream Instead remain deliberate source choices.

Automatic copies require exact catalog/episode/version identity, complete parts,
current library rights, requested track metadata and current quality limits.
Unknown or conflicting identities remain manual-only. Cross-provider series
aliases do not establish corresponding child preference coordinates. Remote
automatic library substitution remains denied for children. Current policy is
confirmed on foreground Play; old active preparation IDs remain usable for
reports/progress. Native library requests retain the initiating profile context.

The feature reads existing scans and permits bounded probes only for actual
preparation. Catalog/status reads add no provider calls or probes. Backup import
cannot manufacture new remote selection proof. No additional migration.

Validation: 3,608 affected backend cases, 348 repeated race cases, 42 API/contract
cases including seven real native serializer fixtures, a real synthetic
PostgreSQL roundtrip and full build. Native: 1,055 tests, 44 new cases, both APKs,
empty private debug prefills, independent final review. Physical playback/focus
acceptance remains separate.

The combined commit `1d74f9e6cf4a49445c9f10be5ac67c91b26f20a0` passes 8,722
backend cases with 38 explicit opt-in skips, including the seven automatic and
13 reminder native contracts, synthetic PostgreSQL and local browser fixtures.
Its built binary has matching clean commit metadata. Packages remain separately
reviewable fork branches; no upstream PR, deployment or application activation
was performed by this work package.


## Exact fork review deltas

These are development deltas against inherited fork bases, not independent
upstream submissions. The combined branch is verification evidence. Any later
proposal still needs extraction onto an agreed upstream base and its own checks.

| Package | Exact reviewed delta |
| --- | --- |
| Automatic library backend | [69cc67ac to 8e9eeebd](https://github.com/stefnats/mediastorm/compare/69cc67ac7d41628be4a2a9eaa8705a65f3c9c5e3...8e9eeebda2fb13bba0e1ef2461cdb0dc6b99638b) |
| Automatic library TV/phone | [70e01fcb to d1250c19](https://github.com/stefnats/mediastorm-androidtv/compare/70e01fcb0ace7cfd9ba345ec946e3695f7a2a2b2...d1250c19fc5cee529aaecc12e03c3b457798da1a) |
| Local historical series aliases | [1d74f9e6 to ccffcfc2](https://github.com/stefnats/mediastorm/compare/1d74f9e6cf4a49445c9f10be5ac67c91b26f20a0...ccffcfc24c1440918bfa619c5da7ab08eb34c31b) |

The two-file alias follow-up recognizes only known numeric same-provider series
anchors without optional cached metadata. It retains exact-episode, current
library access and conflicting-ID checks. Its full localmedia package and
13 new table cases pass; independent review is clean. It was not part of the earlier 8,722-case run; the later 8,818-case
media-evidence integration includes it.
No upstream PR or deployment was made.

## Fork-only media evidence and native display

[Backend](https://github.com/stefnats/mediastorm/tree/feature/media-evidence-foundation)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-media-evidence)
separate catalogue original language, release claims and observed file metadata.
Catalogue facts come from existing normal TMDB movie/series detail responses;
legacy/search-only scalar language does not establish provenance. Catalogue
facts last at most 24 hours; source observations at most 15 minutes. Cache reads
and copies retain original times. Imported/manual metadata cannot forge proof.

Source facts reuse existing searches and full probes. Old path/torrent caches
cannot acquire file provenance. Release and concrete input identities use
separate opaque digests, not content checksums. Movie/explicit-health facts stay
source-only; series binding requires an already verified exact official episode.
Missing HDR/AD stays unknown; clipped raw inventories carry completeness flags.
Container tags do not verify spoken language, content identity, decoded quality
or device output. These facts grant no availability or playback entitlement.

Native details and existing source/track panels display the facts without new
search/probe/prewarm actions. Profile epochs and exact rendered probe snapshots
retire stale responses and dialog actions. TV uses existing expandable text to
retain the track list's initial focus target. Full filters and correct filtered
server pagination remain separate work; physical focus/touch/rendering is open.

Validation: 8,767 backend cases, 58 optional environment skips in the package
run, 84 race cases, server build and full-source build. Native: 1,106 tests,
51 new cases, zero skips, both APKs and empty private debug prefills. Eight
unchanged Go JSON examples (including two actual local handler responses) are
portable native tests; eight native component examples validate on the backend.
Final independent production and fixture reviews are clean.

Exact fork deltas: [backend 1d74f9e6 to b62f5ef1](https://github.com/stefnats/mediastorm/compare/1d74f9e6cf4a49445c9f10be5ac67c91b26f20a0...b62f5ef10ed773e88e3fc6f9bcba0fd7772accbd)
and [TV/phone d1250c19 to 85022e69](https://github.com/stefnats/mediastorm-androidtv/compare/d1250c19fc5cee529aaecc12e03c3b457798da1a...85022e69270b86e9fb722c3ead2bc100f70e00dd).
They inherit fork prerequisites and are not isolated upstream submissions.

Combined commit `33101c78523d2752afe270f6b67ae508da2132d9` includes the local
series-alias follow-up and passes 8,818 cases with 38 explicit opt-in skips,
synthetic PostgreSQL, local browser fixtures and existing native contracts.
The server binary has matching clean commit metadata. No upstream PR, external
comment, provider query, production deployment or application activation.

## Fork-only profile station favorites

[Backend](https://github.com/stefnats/mediastorm/tree/feature/station-favorites)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-station-favorites)
store favorite broadcasts separately from existing stream favorites. Eligible
alternatives remain favorites when another allowed playlist is added. A dedicated
idempotent profile write preserves other settings; old/stale full settings saves
cannot overwrite the station list. Markerless clients retain their old behavior.

Existing server station identities, explicit regional/language/+1 distinctions,
source access and enabled-category policies govern membership. Explicit ISO2
country metadata no longer collapses ES/MX or NL/BE; known German hashes remain
stable. Personal Show Hidden is explicit and grants no source access. In opt-in
requests, eligible favorite pinning/filtering precedes the per-source count cap;
visible grouping counts and pages follow filtering. No stream opens on favorite.

Native group headers and flat/channel/program menus expose distinct station and
stream actions. A shared profile queue orders writes, scopes settings/pages and
retries to the original epoch, and preserves EPG initialization across canceled
startup. Catalog removal does not retune or stop the playing channel.

Validation: 3,479 affected backend cases, 48 repeated race cases, 49 API cases,
real synthetic PostgreSQL JSON roundtrip and full build. No migration. Native:
1,144 tests, 38 new cases, zero skips, both APKs and empty private debug prefills.
Five Go fixtures are portable native tests. A separate six-file test-only step
runs five production native body-builder exports through real handlers, parsers
and OpenAPI (41 contract cases). Method/path provenance is traced separately;
the fixtures are serialized bodies, not captured network traffic. Independent
reviews are clean; physical rendering/focus/touch remains open.

Exact deltas: [backend 33101c78 to 024f92a7](https://github.com/stefnats/mediastorm/compare/33101c78523d2752afe270f6b67ae508da2132d9...024f92a7bf3ee00f0f124b108cdfdd13e00a340c),
[native 85022e69 to e5be1697](https://github.com/stefnats/mediastorm-androidtv/compare/85022e69270b86e9fb722c3ead2bc100f70e00dd...e5be1697da84701c6b29f713f6ecaecfa17d03e5),
and [contract-only 024f92a7 to 9989edb1](https://github.com/stefnats/mediastorm/compare/024f92a7bf3ee00f0f124b108cdfdd13e00a340c...9989edb15c41ab9b85b2a042b0871e4c0b85f409).
These inherit fork prerequisites and require extraction before upstream review.

Combined `9989edb15c41ab9b85b2a042b0871e4c0b85f409` passes 8,839 cases with
38 explicit opt-in skips, synthetic PostgreSQL and local authenticated browser
fixtures. Its server binary has matching clean commit metadata. No private
playlist, provider query, upstream PR/comment, deployment or application start.

## Fork-only DVR capture parts and source failover

[Backend](https://github.com/stefnats/mediastorm/tree/feature/dvr-source-failover)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-dvr-parts)
preserve each recording input attempt in an exclusive owned file. Optional
failover revalidates current station, profile rights and quality before ordinary
private capacity admission. Country, language and time-shifted broadcasts stay
distinct. Old processes join and flush before releasing their connection.
Existing explicitly chosen primary transports retain their authorization path;
automatic alternatives require supported direct M3U/Xtream input.

Parts with different codecs/audio layouts are never appended or remuxed together.
Gaps and interrupted/multipart outcomes remain incomplete. Default playback uses
the lowest nonempty available part, whose HTTP response ends at its own EOF even
if another part is recording. Explicit parts support HEAD/Range. Native closed
part actions start at zero without progress/resume writes or automatic chaining.
Continuous playback, merging and export of every part remain separate work.

Migration 076 adds PostgreSQL capture metadata. Startup marks interrupted ledgers
honestly without fabricating closure times. Restore cancels and joins registered
writers before its transaction, gates new registration and strips imported source
authority. Public responses omit source URLs/private paths/run identifiers;
owned part resolution rejects imported locations, symlinks and forged selectors.
Deletion waits for actual writer cleanup. No production restore or migration ran.

Validation: 3,639 affected backend cases with 42 explicit environment/opt-in
skips, 45 repeated runtime/compatibility race cases, 84 repeated HTTP race cases,
39 repeated planning race cases, API/schema cases and build pass. These reruns
overlap the ordinary cases. Actual local FFmpeg fixtures switch H264/mono to
HEVC/stereo; independent ffprobe and first-part hashes verify preserved files.
Ten real Go handler replies are byte-identical native resources. Native: 1,192
tests, 48 new cases, no skips, both APKs and empty private debug prefills.
Independent reviews are clean; physical rendering/focus/touch remains open.

Exact deltas: [backend 9989edb1 to 7ddae24c](https://github.com/stefnats/mediastorm/compare/9989edb15c41ab9b85b2a042b0871e4c0b85f409...7ddae24cb41e48288b126f499ec3db272391f1ef),
[native e5be1697 to fa893ef3](https://github.com/stefnats/mediastorm-androidtv/compare/e5be1697da84701c6b29f713f6ecaecfa17d03e5...fa893ef329d1e394d8a1295ad42ba360a5bfa0c6).
A [separate one-file test correction](https://github.com/stefnats/mediastorm/compare/7ddae24cb41e48288b126f499ec3db272391f1ef...91859233654469e8a2f864b0575f54dc9fdcce8b)
gives an existing loopback fixture a unique cache URL; unchanged assertions pass
20 repeated runs. Production cache behavior is unchanged. These branches inherit
fork prerequisites and require extraction before any upstream review.

Combined `91859233654469e8a2f864b0575f54dc9fdcce8b` passes 8,936 cases with
38 explicit opt-in skips, synthetic PostgreSQL and
local authenticated browser fixtures. The server binary has matching clean commit
metadata. No private playlist, real provider/indexer query, upstream PR/comment,
deployment or application restart. The next scoped packet is server-side stream
filters over bounded retained snapshots; filter/page actions will remain cache-only.

## Fork-only retained source filters

[Backend](https://github.com/stefnats/mediastorm/tree/feature/source-filters)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-source-filters)
filter the source picker over a server-owned snapshot retained before presentation
and per-resolution caps. At most 512 candidates/8 MiB are retained, with explicit
truncation. Counts cover that retained pool, not every possible provider result.
Filter/page/reapply operations perform no provider, metadata, preparation, health
or probe calls. The normal initial health budget covers only the first visible page.

Release claims and observed file metadata remain separate bases. Missing or expired
traits are unknown; users can explicitly require known matches. File audio language,
codec and description predicates must match one track. Complete inventories are
required for negative conclusions. Unbound series file facts cannot establish a
known exact-episode match; movie facts remain source-only. Neither basis proves
spoken content, decoded quality or device output. Library versions stay separate.

Snapshots bind current account/profile/client, configuration/provider rights,
content preferences and reports. Frozen views retain counts, indices and proof
expiry across pages. New proof invalidates old cursors; reapply stays cache-only.
A deliberate fresh track choice remains usable when only an older view is stale,
while context or permission changes retire it. Existing scoped probes resolve
server-owned rows, ignore caller result claims and recheck context after work.
Prepared metadata requires a private configuration marker captured before input
selection/probing; imported or old entries cannot mint current proof.

Legacy servers retain their existing arrays and source actions. Native strict
decoding rejects coerced enums and malformed scalar fields. A static registered
language table matches the backend normalizer and is packaged in both APKs.
No source result is rewritten into a playback grant.

Validation: 3,736 affected backend cases with 36 explicit opt-in/environment skips,
165 repeated race cases, 40 API/schema cases and build pass. These reruns overlap
ordinary cases. Fourteen actual Go replies are portable native resources; seven
production native request-helper exports pass real handlers and OpenAPI. Only
opaque runtime references are rebound for successful continuation. These are
serialized requests, not captured network traffic. Native: 1,257 tests, 65 new,
no skips, both APKs and empty private debug prefills. Independent reviews are clean.

Exact deltas: [backend 91859233 to c1f04b9e](https://github.com/stefnats/mediastorm/compare/91859233654469e8a2f864b0575f54dc9fdcce8b...c1f04b9ed3f1d220208e7d23f08548bfd49fa11b),
[native fa893ef3 to a26483ed](https://github.com/stefnats/mediastorm-androidtv/compare/fa893ef329d1e394d8a1295ad42ba360a5bfa0c6...a26483edab948e4ed12517ea65d7096922cac23b).
These inherit fork prerequisites and need extraction before upstream review.

Combined c1f04b9ed3f1d220208e7d23f08548bfd49fa11b passes 9,007 cases with
38 explicit opt-in skips, synthetic PostgreSQL and local authenticated browsers.
The binary has matching clean commit metadata. Catalogue-wide/original-language
filtering and physical device acceptance remain separate work. No private playlist,
real provider/indexer query, upstream PR/comment, deployment or application start.

## Fork-only internal regional VOD decision core

This six-file [internal backend foundation](https://github.com/stefnats/mediastorm/tree/feature/kids-vod-foundation)
adds no main wiring, routes, public policy switch, persistence or migration.
Existing guards and playback behavior are unchanged. It is not complete VOD
authorization coverage or family safety acceptance.

Explicit regional policy decisions require current owned profiles, effective
sessions and exact server-owned cached resource/file/episode bindings. Unknown
or imported decisions cannot become a legacy fallback. An explicit disabled
policy produces NotApplicable, never a positive regional grant. Raw TMDB DE
metadata or US scalar ratings do not authorize an edition or episode.

Only fresh parent-confirmed exact-source exceptions can allow. The existing
parent-authentication guard is reused for each mutation; delayed confirmations
recheck ownership/session/resource/policy. Private per-instance capabilities
cannot be imported or transferred. Bounded leases expire at the earliest
parent/source/session boundary and respond to revocation or trusted invalidation
without provider polling. HTTP adapters still need ownership, PIN, source/library
checks and actual byte/provider gate wiring. Cold unresolved sources stay unknown;
no preparation permission or file identity is invented from release hashes.

Validation: 86 ordinary cases including the real parent-authentication guard with
synthetic account verification, 258 repeated race cases, full package build and
server build. Race cases overlap the ordinary suite. Independent source/hash
review is clean. Combined 918510883de83ff1de2827887c941a34654ee24d passes
9,081 cases with 38 explicit opt-in skips, synthetic PostgreSQL/local browser
fixtures and a matching clean-commit server binary.

[Exact six-file delta](https://github.com/stefnats/mediastorm/compare/c1f04b9ed3f1d220208e7d23f08548bfd49fa11b...918510883de83ff1de2827887c941a34654ee24d).
The branch inherits fork prerequisites and requires extraction before upstream
review. Public parent controls, all route/reuse/stream gates, internal transport
binding, native denial handling and safe offline ownership remain separate steps.
No private playlist, real provider, production change or new upstream PR/comment.

## Fork-only native terminal VOD denials

[Native correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-vod-denials)
stops observed terminal backend access denials from becoming alternate-release,
per-episode, offline-resume or stale-details playback attempts. Manual resolution
and polling retain their captured profile context; queued Up Next settings reads
cannot switch to a replacement profile. Only the existing coded profile_locked
response can request PIN recovery.

Player HTTP handling classifies the actual final response origin rather than
the original request URL. Own protected-media denials stop Media3 retry/fallback;
external CDN403 keeps ordinary provider handling. A player-local nonce marker is
never sent over the network and incoming forged markers are removed. Captured
backend credentials are stripped on external redirects while distinct explicit
provider authorization is retained. Synthetic HTTP redirects exercise these paths.

Current source/player denials retire matching already-open detail actions through
a bounded ephemeral event, not a stored policy or permission. Captured epochs,
current-operation tokens and metadata read-start fences ignore old delayed failures
after a newer successful read or reopened page, while newer denials still apply.
Reporting or changing releases cannot bypass a current terminal authorization error.

Validation: 1,298 native tests, 41 new cases, zero failures/errors/skips, TV/phone
APKs and empty private debug prefills. Independent production and frozen47-file
hash reviews are clean. Seven future internal denial codes match the backend core;
no public HTTP policy fixture or regional rule was invented. This is reactive
denial handling, not continuous authorization, offline ownership or full family
safety acceptance. Physical device rendering/focus/touch remains open.

[Exact native delta](https://github.com/stefnats/mediastorm-androidtv/compare/a26483edab948e4ed12517ea65d7096922cac23b...472ca44fead2f65d065abaf24ec9cdf3a2ce436d).
This inherits fork prerequisites and needs extraction before upstream review.
No provider/private playlist, installation, policy activation or upstream PR/comment.
Offline server/account/profile ownership is the next separate native packet.

## Fork-only VOD adapters and offline ownership

The [private backend adapter packet](https://github.com/stefnats/mediastorm/tree/feature/kids-vod-adapters)
adds pure snapshots, current account/session/profile readers, a mandatory PIN
bridge and bounded already-open reader/loopback seams. The optional observer
defaults to nil and observes only an existing verified adult Plex/Jellyfin winner,
its original server catalogue anchor and matched probe. No additional provider
or metadata lookup occurs; current child remote-library denial remains intact.

Private receipts bind process, attempt, profile/PIN, client, settings, reports,
original expiry and current library routing/item/part/episode. PIN callbacks
recheck authority before and after core resolution. Readers check before and
after reads and close blocked input on revocation. Loopback tokens bind actual
TCP peer, exact private input, method, lease and expiry. No trusted opener,
Main registration, public route, persistence or enable switch is installed.
This remains internal groundwork, not complete VOD enforcement or family safety
acceptance. Caller paths, hashes, imported flags and adult receipts cannot
establish a child's source binding.

Validation: 3,703 affected cases with 36 optional skips, 441 repeated race cases,
40 API cases and both builds passed. Repeated/subset cases overlap the ordinary
suite. The combined clean commit passes 9,230 cases with 38 explicit opt-in skips,
synthetic PostgreSQL/local browser fixtures and a matching clean-commit binary.
[Exact backend delta](https://github.com/stefnats/mediastorm/compare/918510883de83ff1de2827887c941a34654ee24d...753897d601c5e22d6e579495a2325d377ec70264).

The separate [native offline ownership packet](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-offline-ownership)
binds new files and progress to the original logical server, account and profile.
Queue, HTTP, files, notifications, player entry and deferred actions check this
context. Old writers are joined before replacement or deletion. Ambiguous and
unknown/future records are preserved without adoption; legacy and foreign files
and progress remain unavailable and are not deleted. Only an authenticated raw
profile response with explicit boolean false supplies transient adult-profile
provenance. Verified home/away routing preserves logical identity; account/session
replacement invalidates the proof.

Material limitation: after a cold launch without current raw adult-profile
verification, even owned adult downloads are unavailable until the original
server/account/profile context is verified again. Children without a current
verified grant cannot play offline. No persistent offline grant or continuous
remote authorization is implemented. All 1,337 native cases passed, including
39 new cases and real deferred reader/file/loopback tests. Both APKs built with
five private debug prefills empty. The existing Go profile handler's 623 response
bytes are consumed unchanged in native raw-parser tests; synthetic service and
server-injected context do not represent a network authentication trace.
Independent review and source/APK/fixture hashes are clean. Physical-device
rendering, focus, touch and playback acceptance remain open.
[Exact native delta](https://github.com/stefnats/mediastorm-androidtv/compare/472ca44fead2f65d065abaf24ec9cdf3a2ce436d...22e1cbe574f3636793ed3bab9c15607992ebbb97).

Both branches inherit fork prerequisites and need isolated extraction and scope
agreement before upstream submission. No new upstream PR, service activation,
deployment, private playlist use or real provider/indexer request occurred.

## Fork-only original player and Cast ownership

The [native correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-playback-ownership)
retains the original profile epoch from preparation through pending player entry,
transport and deferred Cast work. A delayed activity cannot adopt an old URL
under the replacement profile. Open players retire when their original context
changes. Trailer preparation, polling and serve URL construction also retain
that original context; cancellation propagates before another profile's request.

Cast arming, handoff, load, status/progress, keepalive and cleanup carry the same
owner plus a local start generation. Old rendered controls and callbacks cannot
command a replacement. Pending same-receiver loads remain serialized until the
actual transport reply. Cleanup targets the captured receiver/session; an invalid
old server context never rebuilds a cleanup request with replacement headers.
Old server HLS sessions may therefore wait for their existing expiry. This does
not revoke an already delivered remote URL. Local-file, restricted and unknown
profile casting remains unavailable without a supported permission.

All 1,372 native cases passed, including 35 new cases, actual local HTTP/redirect
checks, controlled deferred receiver replies and original trailer orchestration.
Both APKs built, five private debug prefills are empty, and source/APK/registry
hashes were checked independently. The 37-file final freeze supersedes the
preliminary 35-file state: the trailer preparation gap was corrected before
publication and the full suite/builds were repeated.
[Exact delta](https://github.com/stefnats/mediastorm-androidtv/compare/22e1cbe574f3636793ed3bab9c15607992ebbb97...0a15453cabc9bcfbcf3dba733019c74e5710065a).

This branch inherits fork prerequisites and requires isolated extraction before
upstream review. No new public authorization protocol, persisted grant or age
policy was introduced. Other auxiliary endpoint ownership, server transport
coverage and protected parent controls remain follow-up work. Physical receivers,
rendering, focus and touch were not tested. No provider, private playlist, device
installation, production activation or new upstream PR/comment occurred.

## Fork-only preparation boundaries and thumbnail transport

The [dormant backend preparation port](https://github.com/stefnats/mediastorm/tree/feature/vod-boundaries)
checks current account applicability before profile selection or provider work
in resolve, batch, queue and prequeue/retry/adoption/status paths. Unknown/error
state never becomes inactive. Configured full sessions require an explicit
profile matching the stored owner; caller titles, paths and result attributes
cannot choose an owner. Bounded body/query/path selectors must agree, including
duplicate selector occurrences. Canonical checked profile/client values reach
the actual downstream operation.

Private worker correlation retains current session/PIN/policy revisions and the
original expiry. Superseded workers cannot overwrite successor setup, progress,
source, evidence or Ready state. Parent cancellation, values and shorter probe
timeouts are retained. A distinct cleanup-only context can cancel owned work
during a policy lookup outage without authorizing demand or becoming an inactive
fallback. Old/unbound queues and child cold preparation remain unsupported.
No public policy, constructor installation, Main route or persistence was added.
Video/HLS, auxiliary/delegated media, actual input binding, protected parent
controls and cold preparation permits remain follow-up prerequisites.

Validation: 3,926 affected cases with 36 optional skips, 366 repeated race cases,
40 API cases and both builds passed. Repeated/subset counts overlap. The combined
clean commit passes 9,352 cases with 38 explicit opt-in skips, isolated PostgreSQL,
local browser fixtures and an exact matching clean-commit server binary.
[Exact nine-file delta](https://github.com/stefnats/mediastorm/compare/753897d601c5e22d6e579495a2325d377ec70264...f4d5bdf035e18c36b6fa11800208193a11f1443a).

The [native thumbnail correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-thumbnail-ownership)
replaces global headers/loader use in both player scrubbers and prefetch paths
with an original-owner pipeline. Backend credentials are attached only to the
exact own scheme/host/port, reverse-proxy prefix and registered image route.
Every redirect checks the destination. External queries and distinct provider
authorization remain intact; exact captured backend secret query values in an external URL
cause rejection before HTTP. Relative query parameters are preserved.

Identity/lifetime checks cover request creation, network results and the owned
cache adapter. All 1,390 native cases passed, including 18 new cases and real
synthetic two-origin HTTP checks. Both APKs built; five private prefills are empty,
and source/APK/registry hashes were checked independently.
[Exact nine-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/0a15453cabc9bcfbcf3dba733019c74e5710065a...7ad505e1c1e64295b4af425a93b63d7d108e1ef1).
The tests do not claim actual Coil/device instrumentation, remote age grants or
full auxiliary enforcement. Global artwork outside this dedicated thumbnail
pipeline remains a separate follow-up.

Both branches inherit fork prerequisites and need isolated extraction plus scope
agreement before upstream submission. Independent production/frozen-source
reviews are clean. No new upstream PR/comment, private playlist use, real provider
request, service activation, deployment or physical-device test occurred.

## Fork-only global artwork credentials

The [artwork correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-artwork-credentials)
binds account and profile-unlock headers to the exact backend origin, reverse-proxy
prefix and protected library artwork route. Public image routes retain only the
transport credentials required by paired access. Unrelated API routes receive no
helper-injected backend credentials. Both application image-loader factories use
the corrected client.

Each network hop removes only credentials actually injected by the helper when
the destination no longer permits them. Distinct provider authorization remains
intact. Exact captured secret query values on external URLs are rejected before
HTTP; this is not arbitrary URI/path/userinfo scanning. Query parameters and proxy
paths remain preserved. The snapshot starts at HTTP call creation, without adding
a caller-wide ownership protocol, cache/content authorization or age grant.

Validation: all 1,410 native tests passed, including 20 new cases and synthetic
HTTP redirect checks. Both APKs built. Source manifests, raw XML, APK hashes,
packaged registry and five empty private debug prefills were independently checked.
[Exact six-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/7ad505e1c1e64295b4af425a93b63d7d108e1ef1...05dc364b873c4f0d03917b39c8070ff7f0b4757f).
No actual Coil/device instrumentation, production activation, real provider access
or new upstream submission occurred. Original preparation-operation context is
the next native work package.

## Fork-only recording owner access

The [recording privacy correction](https://github.com/stefnats/mediastorm/tree/fix/recording-owner-access)
authorizes the repository-stored recording owner before shared Video aliases or
the recording stream handler can read media, cached metadata or direct file URLs.
The current full session, actual account role and expiry, stored owner profile
and current profile PIN policy must agree. Missing guard dependencies, unknown
owners and conflicting profile selectors fail closed. This fix is wired in Main
independently of the still-dormant regional VOD port.

Three actual pre-fix regression cases returned another account's synthetic file
bytes, private cached metadata and direct path. All are denied after the fix;
owned/current-master GET, HEAD, Range, cache and direct URL controls still pass.
Repository/session authorization snapshots avoid file projection, provider
validation and session renewal before the decision.

Scoped recording sharing/party/Jellyfin aliases are deliberately unavailable
until their issuers prove the recording owner and PIN. Non-recording sharing
retains existing behavior. The old internal-stream exception survives only through
a dedicated actual-loopback-peer callback and private context marker; public URLs
and forwarded headers cannot mint it. This preserves local-process trust, without
claiming an owner/input-bound capability. Full regional enforcement, long-lived
revocation, HLS/auxiliary/delegation/WebDAV coverage and protected parent controls
remain separate prerequisites.

Validation: 3,841 affected cases passed with 36 existing optional skips, 291 repeated
race cases, 36 OpenAPI cases and both builds passed. Another 60 overlapping root
race events cover real aliases and the thumbnail fallback URL builder. Existing
DVR part Range/HEAD/EOF and local FFmpeg/growing-HLS tests remain green. Counts of
subset and repeated runs overlap. Independent source, frozen manifest, archive,
raw test and binary checks are clean.
[Exact fifteen-file delta](https://github.com/stefnats/mediastorm/compare/f4d5bdf035e18c36b6fa11800208193a11f1443a...9846f6bd4bf2dc76615ac2b5b0b47b7201c55c1e).

The combined clean commit passes 9,423 cases with 38 explicit optional skips and has an exact matching clean-commit server binary. All fixtures are synthetic. No production service, real provider, private playlist, physical device or new upstream submission was used.

## Fork-only original preparation context

The [native preparation correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-preparation-context)
retains one original playback context through report options, report/retry,
replacement status, cancellation and undo. Every operation checks the original
epoch before and after suspension and supplies the explicit profile selector from
that same epoch. Stale cleanup sends no request with replacement credentials.
The player also retains its original context when saving audio preferences.

Warm preparation now reads settings with each operation's captured epoch, rather
than a later default-current context. A retired operation cannot start demand;
a new operation on the same resolver uses the fresh epoch. Source-health requests
also carry the captured profile selector through the existing protocol.

Validation: all 1,424 native tests passed, including 14 new cases; both APKs built.
Independent review and root verification checked all 10 source files, raw XML,
APK hashes, packaged registry and five empty private debug prefills.
[Exact ten-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/05dc364b873c4f0d03917b39c8070ff7f0b4757f...949ff0a801cdb14466224d113f9ea699bb169eda).

Epoch-retirement tests simulate transitions with two actual invalidations while
profileId remains unchanged; they do not activate the Android profile picker.
Successful local HTTP tests use the production query/context transport helpers.
Actual ApiClient methods prove stale rejection before Android credential access
and zero HTTP requests, without claiming a successful full Android ApiClient.send.
Real gateway/report flows use deferred request fixtures; no physical controller,
decoder, device, production service, provider or new upstream submission was used.
No new content or age grant is introduced.

## Fork-only skip-segment request context

The [skip-segment correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-skip-context)
retains the original player epoch and profile through intro/recap/credits lookup,
chapter fallback and repeated marker lookup. Each stage checks its generation
before and after suspension. Terminal access denials retire the operation and
use the existing player-denial path without chapters or another lookup. Ordinary
lookup failures retain the existing fallback. Retired or replaced-duration
responses cannot overwrite newer ranges.

Validation: all 1,439 native tests passed, including 15 new cases; both APKs built.
Independent source/frozen-artifact review and root checks confirmed all six files,
raw XML, APK hashes, registry and five empty private debug prefills.
[Exact six-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/949ff0a801cdb14466224d113f9ea699bb169eda...5d9b482863a6472ad7c264da8ec5a2a42464e27a).
The epoch transitions are simulated without Android profile-picker activation.
Successful HTTP uses the production query/context helper seam; actual ApiClient
methods prove stale rejection before HTTP, without claiming a successful full
Android send, decoder or device test. No production or provider request occurred.
Subtitle search/extraction/translation is the next independent native package.

## Fork-only subtitle operation ownership

The [subtitle correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-subtitle-context)
retains the original player owner and private per-intent tickets through settings,
search, download, extraction, translation, parsing, refresh and adoption. Actual
TV/phone search effects use the same owned adapter without UI changes. Search
rows bind by object identity and exact language; copied or superseded results
cannot acquire a new ticket. Existing UI sorting retains original references.

New manual choices retire previous automatic/download/translation work and cancel
the registered in-flight translation refresh. Track refresh compares the actual
shown object rather than a repeated display label. Terminal denials stop English,
provider and extraction/translation fallback and use the existing guarded player
abort. UI effects end through controlled cancellation. Ordinary search failures
retain the existing clearly labelled English embedded-track fallback.

Unreadable subtitle settings leave providers/translation unavailable; exceptions
never silently enable translation. Cancellation and typed authorization failures
remain distinct. Track-default loading retains one expected context and guards
publication without causing an uncaught settings-screen effect failure.

Validation: all 1,466 native tests passed, including 27 new cases; both APKs built.
All ten source files, raw XML, APK hashes, registry, empty private prefills and
actual consumer provenance were independently checked. The preliminary 1,464-test
run was superseded before publication.
[Exact ten-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/5d9b482863a6472ad7c264da8ec5a2a42464e27a...98741671d25f5e6133c25a2f7712be0ccb641f7e).

Actual adapter cancellation hooks are exercised with deferred runtime tests.
Call.await cancelling its OkHttp call is separately source-verified, without
claiming that complete Android ApiClient.send or physical playback was exercised.
Successful HTTP tests use the production query/context helper seam; actual API
methods prove stale rejection before HTTP. No new server content/age grant,
production service, real provider, private playlist, device or upstream submission
was used. HLS control context is the next paired audit.

## Fork-only raw video ownership

The [uninstalled raw video port](https://github.com/stefnats/mediastorm/tree/feature/vod-video-boundary)
checks current account applicability and original owner before Stream, Probe and
DirectURL cache access or source work. Unknown state and active children without
authoritative strict input proof fail before demand. Active adult correlation is
NotApplicable to a regional child grant, and never supplies catalogue/file proof.
Original session/request expiry is retained without extending reads or changing
the existing preparation lifetime.

Configured requests bypass path-only byte/metadata caches, prefix spool, shared
probe work, persistent stream pools and redirect reuse. Actual provider and HTTP
opens, composed redirect callbacks, returned bodies and FFprobe start/output are
checked against that same owner. Returned handles are checked before/after reads;
late bytes are discarded and only the owned body is closed. Current existing
library/recording ACL is rechecked with bounded repository-only lookups, followed
by another owner check. Known library paths with missing ACL dependency deny.
Cached item-to-library mapping is not fresh reassignment or transport/file proof.

The first late denial restores the original HTTP header baseline, preserving CORS
while dropping prepared private validators and media headers. Real failing tests
were retained for the original prepared-header leak, a redirect-policy callback
that retired the owner before another HTTP open, and the historical double-close
of a redirect body already closed by net/http. Provider error responses retain
their separate owned-body disposal contract.

Actual synthetic HTTP tests cover allowed and denied two-origin redirects, late
headers/bytes, body disposal and blocked-read cancellation reaching the server.
Provider tests cover noncooperative late reads, trusted private invalidation,
parent cancellation and expiry. An installed FFprobe parses synthetic PCM WAV;
unknown piped duration remains unknown. Optional configured Blu-ray companion
reads are skipped before I/O, with a nil-port positive control.

Validation: 3,978 affected cases passed with 36 existing optional skips, 324 repeated race cases, 36 OpenAPI cases and both builds. Repeated/subset counts overlap. The combined clean commit passes 9,511 cases with 38 explicit optional skips and an exact clean-commit server binary. Independent source/archive/raw-test/binary review and root frozen-source verification are clean.
[Exact eight-file delta](https://github.com/stefnats/mediastorm/compare/9846f6bd4bf2dc76615ac2b5b0b47b7201c55c1e...d0920c9520fd78f04eb6108e3ca399a2209b1a29).

No Main/API route, public policy, database or HLS lifecycle installation is added.
Some unbound shared probe callers conservatively fail closed when the test port is
configured; independent HLS/auxiliary/delegation/WebDAV opens and protected parent
controls still require their own work. Configured cache bypass and absent companion
enrichment may increase latency and leave languages unknown; no performance target
is claimed. An issued adult DirectURL remains nonrevocable. Blocked-input proactive
cancellation requires the original parent/deadline or trusted private invalidation;
no new production invalidation hooks or complete continuous revocation are claimed.
Nil-port legacy behavior remains unchanged. All fixtures are synthetic; no real
provider, playlist, device, production activation or new upstream submission was used.

## Fork-only native HLS URI context

The [HLS URI correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-hls-uri-context)
builds Live/DVR/Cast media URLs under the original playback epoch. Exact owned HLS
assets carry the captured profile on every Media3 network hop, including relative
segments, variants, initialization and subtitle/caption assets. A child URI does
not inherit its parent playlist query; the actual child request is checked.

Scheme, host, port and reverse-proxy API prefix must match. All existing profile
selectors, case variants and duplicates must agree; conflicts reject before I/O.
Encoded unrelated queries and server-supplied authentication remain unchanged;
captured token/PIN are appended only when absent. External/provider URLs invoke
no native credential callback and receive no added selector. Redirect header
cleanup preserves distinct provider authorization and strips only captured backend
credentials outside the owned API namespace. Non-media control routes gain no
asset selector merely from a matching path prefix.

Validation: all 1,483 native tests passed, including 17 new cases; both APKs built.
Independent review and root verification checked all seven source files, raw XML,
APK hashes, registry, empty private prefills and actual consumer/server-route
provenance. Synthetic two-origin HTTP exercises the actual per-hop interceptor.
[Exact seven-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/98741671d25f5e6133c25a2f7712be0ccb641f7e...c5e196312e5a99780b7f1151b16d87ccc3748fea).

No successful complete Android start/send, decoder, device or receiver is claimed.
Profile URI fields confer no session/input/content/age grant. Cast receiver child
rewriting and owned server leases remain server responsibilities; native HLS
keepalive/restart lifetime is a separate following packet. No provider, private
playlist, production activation or new upstream submission occurred.

## Fork-only stored HLS session boundaries

The [dormant stored-session HTTP port](https://github.com/stefnats/mediastorm/tree/feature/hls-session-boundary)
checks a pure private runtime snapshot before GetSession/activity updates, cached
playlist/segment/subtitle reads, status or keepalive. Current session/account,
explicit profile matching the stored owner, PIN and existing source ACL must
remain valid through waits and response emission. Active child, unknown, unowned
and imported contexts cannot acquire readable authority from session metadata.
A valid request may touch activity before a later denial; late output is withheld,
without claiming earlier legitimate touches or emitted bytes can be undone.

A separate CleanupOnly operation allows exact owned stop during policy outage or
a revoked/stopped runtime without granting playback. Snapshot instance and
generation must still match. Cleanup captures the original physical path before
callbacks, so an old stop cannot delete a replacement. An actual revokeLiveSession
regression exposed owned stop403; readable and cleanup validity are now separated.
The original failing case and superseded pre-fix runs remain preserved.

All three normal creators use private one-shot per-instance directory claims and
discard only unpublished failed/cancelled claims. Startup failure after normal
publication detaches that exact instance. Public IDs/URLs are unchanged; private
directory names and permissions change. Normal IDs were already random: equal-ID
replacement tests are defensive private runtime scenarios, not observed collisions.
These claims establish runtime/cleanup identity, never actual input or age proof.

Validation: 4,082 affected cases passed with 36 existing optional skips, 372 repeated
race events (312 new HLS, 60 existing preparation), 36 OpenAPI cases and both builds.
The combined clean commit passes 9,615 cases with 38 explicit optional skips,
isolated PostgreSQL and local browser fixtures, and has an exact matching server
binary. Independent frozen source/patch/archive/runner/raw-test/binary review and
root verification are clean. Counts of subsets and repetitions overlap.
[Exact six-file delta](https://github.com/stefnats/mediastorm/compare/d0920c9520fd78f04eb6108e3ca399a2209b1a29...60e2fc321283d59cecf795f8600277ded2fb5805).

No Main/API route, policy setting, database or public activation is installed.
Actual HLS source/probe/provider/FFmpeg creation and background seek/restart/
recovery/companion work still need P3B. Configured unported stable-copy fMP4,
seek, caption/on-demand extraction and optional Cast/pruning observers remain
unsupported or skipped; nil-port HTTP behavior stays legacy. Periodic manager
cleanup and Shutdown remain internal ID calls, without a complete worker/input
ownership claim. No performance/storage/progress compatibility target is claimed
for the configured test port.

Headerless receiver child URLs still need server profile/delegation rewriting;
hlsChildAuthQuery currently contains token/PIN only. The native per-hop selector
is separate and does not close receiver transport. Parent controls, cold prepare,
auxiliary/delegated/bridge/WebDAV coverage and actual source proof remain activation
prerequisites. All data/media/servers were synthetic, without real providers,
private playlists, devices, production changes or new upstream submissions.

## Fork-only native HLS operation lifetime

The [native lifecycle correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-hls-lifecycle)
binds recording restart, catchup, retune, preference/identity lookup and Cast work
to the original owner, player lifetime and private operation stamps. Released or
superseded actors cannot adopt late starts or continue fallback. Known resource
receipts protect replacement/detached IDs; cleanup uses the original still-current
context, without borrowing replacement credentials. A start aborted without a
returned ID may still rely on the existing server expiry.

Strict keepalive is explicitly enabled only by the migrated consumers. Genuine
own generated HLS asset401/403 and recognized structured own raw policy/PIN failures
are terminal; uncoded forwarded provider403 and external-CDN403 retain their
behavior. Known denial retires the captured receiver and player actors, without
provider/Stremio/live fallback. Cast and local progress gates cancel registered
child jobs even beneath a NonCancellable parent and prevent late success/migration/
offline publication. Normal stop retains its permitted last closing report.

Validation: all 1,523 native tests passed, including 40 new cases; both APKs built.
Independent source/frozen-artifact review and root verification checked all 17
files (12 production), raw JUnit, APK hashes, registry, private prefills and actual
consumer provenance.
[Exact seventeen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/c5e196312e5a99780b7f1151b16d87ccc3748fea...00981c0b3846ad9fd5540a1b3700b3b4e7429bf6).

Real local HTTP tests exercise Call.await cancellation through the CastOwnership
child boundary. The actual local ProgressReporter consumes a separately deferred-
tested gate; this does not claim a complete Android reporter/send, decoder,
receiver or physical-device execution. Epoch transitions are simulated and completed
server writes cannot be rolled back. Multiscreen consumers remain a separate
following ownership packet; their legacy default is preserved here. No new API,
content/age/delegation grant, provider access, private playlist, production
activation or upstream submission occurred.

## Fork-only Multiscreen ownership

The [Multiscreen correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-multiscreen-ownership)
retains the original playback owner through a private one-shot launch envelope,
capacity/EPG calls, each tile, keepalive, resume and known-session cleanup.
Stale launches cannot create a model or network request. Retune and Remove retire
old tile callbacks immediately; cancellation of one operation leaves the serial
worker available for cleanup. Original known-ID receipts protect replacement and
detached sessions. Own HLS policy/PIN denial is terminal; ordinary provider
failure retains its behavior and failed-stop cleanup can still be retried.

Saved layouts use credential-free configured-server/account/profile namespaces.
Legacy profile-only private channel data is preserved without automatic adoption.
Paired/home-direct/tunnel or unknown configured identity cannot save or resume a
layout until Core has a trusted stable server identity; fresh manual selection
remains available.

Validation: all 1,562 native tests passed, including 39 new cases; both APKs built.
Independent source and frozen-artifact review verified all 16 files (12 production),
raw JUnit, APK hashes, packaged language registry and five empty private prefills.
[Exact sixteen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/00981c0b3846ad9fd5540a1b3700b3b4e7429bf6...b5e3dd73564c9c48c0fb3d252adb401744360687).

Tests execute production session queues, launch/storage/start/tile guards and local
HTTP shared interceptors. Epoch changes are simulated. SharedPreferences wiring is
source-reviewed, not instantiated; no Activity, decoder, rendered frame, physical
device, real login or successful full Android send is claimed. An interrupted
start without a known usable original-owner cleanup may rely on server expiry.
No public content/age grant, private playlist, real provider, production activation
or new upstream submission occurred.

## Fork-only native startup timing

The [native timing foundation](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-startup-timing)
carries one private original-owner observation from the actual Coordinator entry
through PlayerSession to the claimed PlayerController. Success is completed only
by its onRenderedFirstFrame callback. Failure, cancellation, release, owner loss,
timeout and external handoff remain separate once-only outcomes; stale/repeated
callbacks cannot complete a replacement attempt. The monotonic local history is
bounded to 128 anonymous samples, without content, owner, URL or credential fields,
network endpoint or persistence.

Input labels state only whether ready preparation, resolved input or direct input
was supplied. They do not infer warm/cold cache behavior from duration. Automatic
unknown cases stay unknown. This is Coordinator-entry timing, not a measured
physical touch timestamp, and external Cast handoff is not first-frame success.

Validation: all 1,582 native tests passed, including 20 new deterministic cases;
both APKs built. Independent review and root verification checked all five files
(four production), raw JUnit, APK hashes, registry and five empty private prefills.
[Exact five-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/b5e3dd73564c9c48c0fb3d252adb401744360687...615ae62aefea71afaf18590ccb4ebc0223763130).

Tests run the actual production recorder, input classifier and player adapter.
Coordinator and Android Listener wiring are source-reviewed; no physical touch,
Activity/decoder render, device p95, measured warm/cold performance, tile, audio-only
or Cast rendering result is claimed. Device benchmarking remains outstanding.
No real provider, private playlist, production activation or new upstream
submission occurred.

## Fork-only single-reader HLS input

The [dormant HLS input port](https://github.com/stefnats/mediastorm/tree/feature/hls-input-boundary)
captures the original full-session owner before source demand, retains one actual
GET/body and probes at most a private 1MiB prefix under a 10s setup budget. FFmpeg
then receives that exact prefix followed by the same remaining reader. A one-shot
setup-to-runtime handoff preserves the original absolute authority expiry without
renewal; successful handoff survives normal HTTP request completion. Owned process
pipes close and join their source copier on child exit, cancellation or retirement.
Configured child/unbound/unsupported requests fail before input work.

This first subset supports actually observed H264 8-bit420 plus supported AAC audio
into ordinary TS output. It is not immutable-file, whole-stream, title/episode,
regional age, delegation or general HLS compatibility proof. Main remains
uninstalled. Live/YouTube, seeking, recovery/restarts, separate companions/captions
and further input attempts stay unsupported in this configured path. Same-session
known receiver assets retain checked original auth/profile and unambiguous required
PIN, with canonical query keys and no external credential decoration. Generation
fences preserve currently owned output even when old exact cleanup refuses it.

Validation: 4,157 affected tests passed with 36 optional skips, 597 repeated race
checks, 36 OpenAPI cases and both backend builds. Independent review and root checks
verified all nine files (six production), patch, archive members, actual process
binaries, four runners and raw counts. Real FFmpeg/FFprobe, local HTTP and FIFO
canaries verify confinement and unsafe positive controls. The query-only receiver
walk uses actual handlers/middleware with injected synthetic auth, not real login,
TCP receiver or physical-device execution. Historical executed RED logs are retained;
exact old WIP source was not separately archived and is not reconstructed.
[Exact nine-file delta](https://github.com/stefnats/mediastorm/compare/60e2fc321283d59cecf795f8600277ded2fb5805...2ad63a73967bb327e09a836a80df5de15bcbdd80).

The first full integration exposed two old synthetic station fixtures reusing
cached loopback-port URLs. The separate [one-file test correction](https://github.com/stefnats/mediastorm/tree/test/station-fixture-isolation)
adds fresh fixture-only URL namespaces and asserts first fetches, preserving all
station behavior and production code. Its seven existing tests passed 20 normal
and five race repetitions (140/35 events). No cache contents were read or cleared.
The complete clean combined commit5f9afdd2d473b638f78a10cd2d97638d317d8687 passes
9,690 tests with 38 optional skips, isolated PostgreSQL/local browser fixtures and
a server build carrying that same unmodified revision. The failed earlier full
run remains separate evidence. No production, real provider, private playlist,
public family activation or new upstream submission occurred.

## Fork-only saved-list original-language filters

[Backend](https://github.com/stefnats/mediastorm/tree/feature/catalogue-language)
and [TV/phone](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-catalogue-language)
filter the complete visible saved Watchlist, Custom List or Permanent Prequeue
before sorting, effective total and pagination. Up to 32 registered canonical
ISO639-3 codes may be selected, with unknown as an explicit alternative or the
only selection. Original catalogue language does not prove an available audio
track. Unsupported active queries fail visibly; remote discovery, Library and
other unsupported lists do not display ineffective controls.

Facts come from current cached provider observations bound to the saved typed ID
or an existing unambiguous raw-provider mapping, never saved side IDs or manual
language scalars. No extra source/probe/provider search or timestamp refresh is
introduced. Existing cache housekeeping and baseline list enrichment remain.
Normalized stored HomeView/sentinel lists retain their complete index beyond the
old discovery 500-window. Hidden entries are removed before facts or pagination.
No immutable across-page dataset or file/age grant is claimed.

Native controls require a strict boolean support marker in the same reply; there
is no extra capability request or silent fallback to unfiltered pages. Original
profile, immutable options and request generations fence page, reload and Random
results. Permanent Prequeue accepts real ranked and legacy flat shapes, failing
whole malformed pages rather than dropping rows and corrupting offsets. Other
nested legacy adapters and list-management writes remain separate follow-ups.

Validation: backend 4,394 affected cases passed with 39 optional skips, 342 repeated
race cases, 36 OpenAPI cases and both builds. All 1,621 native tests passed,
including 39 new cases, and both APKs built. Independent review/root verification
checked all 16 backend/four production and 23 native/seven production files,
patches, archives, raw reports, APKs and five empty prefills. Ten actual Go responses
and six actual native query-helper exports match byte-for-byte in both directions.
Tests exercise real handlers/OpenAPI, actual metadata cache readers, synthetic
accounts/session/ownership/PIN middleware, native helpers/model/decoder and a
600-matching-title saved-list case. They do not record an Android send, network
login, physical UI or device playback.

[Backend delta](https://github.com/stefnats/mediastorm/compare/2ad63a73967bb327e09a836a80df5de15bcbdd80...1dfcf8b4924025950cb37ae5d13ec1e5bd496963)
and [native delta](https://github.com/stefnats/mediastorm-androidtv/compare/615ae62aefea71afaf18590ccb4ebc0223763130...3a0598006b5ce26fb68289c0528c7853b4f8b1eb).
The clean combined commita32e978f3439d17f5476bf840c46dddb7c275332 retains the separate
station fixture isolation and passes 9,804 cases with 38 optional skips and 106
successful packages, isolated PostgreSQL/local browser fixtures and a server build
carrying that same unmodified revision. No production activation, real provider,
private playlist or new upstream submission occurred.

## Fork-only native list-dialog write context

The [list-write correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-list-write-context)
carries the original dialog/list/target owner and private operation identity
through rename, delete, create/add, membership and refresh. Deferred old work cannot
capture replacement credentials or publish into the replacement Store, picker or
page. Cancellation stays cancellation. Actual page and referential picker identity
prevent an old result from closing a new equal-valued target. An already accepted
remote create may leave an empty list in the original profile; it is not rolled
back with replacement credentials. SetAside/TitleActions legacy entrypoints remain
a separate follow-up, not an all-writers migration claim.

Validation: all 1,658 native cases passed, including 37 new ones, and both APKs
built. Independent source/frozen review and root verification checked all 16 files
(13 production), raw XML, APK hashes, registry and five empty prefills. Tests run
actual receipts, Store/revision/serialization, request constructors and shared
Call.await over loopback. Complete successful Android ApiClient.send and Compose/
Activity navigation are source/compile-reviewed, not executed. Actual historical
picker-reference RED XML/logs are retained; exact old WIP source was not separately
archived or reconstructed.
[Exact sixteen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/3a0598006b5ce26fb68289c0528c7853b4f8b1eb...482460592ef0cbb307077034165ae76e75a482e8).

## Fork-only saved-pair layout namespace

The [two-file namespace correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-saved-pair-namespace)
checks saved and unreadable pairing state before route/account identity. A
disconnected paired bridge cannot fall back to a previously typed server's layout
namespace. Unknown pairing or identity causes no layout-storage read/write;
existing layout data is retained and fresh manual selection remains possible.
Cancellation is preserved and this bookkeeping never grants media authority.

All 1,664 native cases passed, including six new predicate/storage cases; both APKs
built. Independent review/root verification checked both source files, patch,
reports, APKs, registry and five empty prefills. Zero I/O refers specifically to
layout storage. Android SecureStore identity/pairing reads remain source-reviewed;
the JVM tests inject those readings. This is a source-found correction followed
by tests, not an asserted pre-fix executed RED or device run.
[Exact two-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/482460592ef0cbb307077034165ae76e75a482e8...46fbc147e5faa8788eda20bbcc0b71ca07410361).

Both packets remain only in the fork. No production activation, real provider,
private playlist or new upstream submission occurred.

## Fork-only optional automatic provider limits

The [optional provider cap](https://github.com/stefnats/mediastorm/tree/feature/auto-provider-limits)
defaults off. An explicit authenticated check of a saved global source can supply
a fresh additional cap for 60 seconds. It never raises a positive manual limit;
expiry, opt-out, source changes and unknown reports fall back to manual policy.
Admission does not poll providers. Existing live/DVR streams and upstream reuse
remain intact. A denial caused by this additional cap cannot authorize takeover.
The existing host/username reservation bucket is coarse: private source aliases
may inherit an additional denial, not a new transport-specific capacity grant.
Provider totals are not used to calculate foreign application occupancy.

All 4,181 component cases passed, with 21 optional skips; 324 race cases and 36
API contract cases passed. The integrated backend passed 9,833 cases with 38
optional skips across 106 packages and built from the clean recorded commit.
Actual embedded settings/browser checks cover save, explicit checks, expiry and
opt-out with synthetic sessions and zero automatic provider calls. A real UI
check found the missing visible-field whitelist entry, which is corrected.
Historical browser socket-length and isolated reload-transport failures remain
documented; they are not reported as successful original runs.
[Exact sixteen-file delta](https://github.com/stefnats/mediastorm/compare/a32e978f3439d17f5476bf840c46dddb7c275332...1c245bda189dba35729f8624f10491468bb3aa52).

## Fork-only completed DVR parts queue and local resume

The [native DVR queue](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-dvr-part-queue)
plays the selected snapshot of completed, nonempty parts in their original index
order. Each next controller starts after the previous controller releases, under
the original playback owner. Cancellation, failure or owner loss retires the
remaining queue. Local resume records the original part and position separately
from VOD history. An unchanged part keeps its resume position when another part
finishes later; the running queue never silently appends that new sibling.

All 1,701 native cases passed, including 37 new cases, and both APKs built.
Root and independent review verified all 17 files, raw XML, APK hashes, registry
and five empty prefills. Tests consume the actual Go capture response fixtures
and execute native parser, queue and cursor helpers. Android Activity/Media3
handoff, clock, persistent identity and device codec transitions remain source/
compile-reviewed. There is no device playback, gapless transition, global measured
timeline, export/remux or cross-device resume claim.
[Exact seventeen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/46fbc147e5faa8788eda20bbcc0b71ca07410361...66f2255302ed9b86e63f25d6d41b067b02f41915).

Both packets remain only in the forks. No production activation, real provider
request, private playlist publication or new upstream submission occurred.

## Fork-only explicit provider occupancy uncertainty

The [capacity report](https://github.com/stefnats/mediastorm/tree/feature/provider-occupancy-uncertainty)
now explicitly returns unknown external usage. Provider account totals and local
reservations lack common trusted measurement time and per-client ownership, so
the API never subtracts them or invents an outside count, range or zero. A valid
provider aggregate zero is retained as an observation independently of a missing
limit. Cached provider check completion and freshly sampled local completion are
labelled separately; neither is represented as provider measurement time.

All 4,214 affected cases passed with 21 optional skips, plus 282 race cases,
36 API contract cases and both component builds. Actual embedded settings-browser
checks run all three existing manual/automatic/uncertainty flows, including cached
timestamps, expiry and zero automatic provider calls. Root and independent review
verified all six files (three production), full patch/archive, binary, runner
hashes and raw reports. The clean integrated backend passed 9,866 cases with 38
optional skips across 106 packages and built at the same recorded commit.
No admission, manager, configuration or transport policy is changed here. This
closes honest reporting of uncertainty, not measured external occupancy.
[Exact six-file delta](https://github.com/stefnats/mediastorm/compare/1c245bda189dba35729f8624f10491468bb3aa52...0892cd709fdf9fc7d8dcaec69d3595d5ba2027a0).

The packet remains only in the fork; no production activation, real provider,
private playlist or new upstream submission occurred.

## Fork-only closed DVR parts ZIP export

The [backend export route](https://github.com/stefnats/mediastorm/tree/feature/dvr-parts-export)
downloads terminal capture-v1 recordings as ZIP Store archives of their closed
original parts and a sanitized manifest. Original indices, stored incomplete
flags, observed gaps and mixed codecs are retained. Nothing is concatenated or
remuxed. Current full-session, stored-owner and PIN checks precede file access
and remain bound to the original identity throughout reading and output.
Two transfers are allowed at once. Exact opened regular descriptors, fixed
sizes and bounded one-MiB buffers avoid reopening untrusted ledger paths.
Failure after output starts aborts the transfer without finalizing a valid ZIP.
This is an API addition; export buttons remain a separate UI packet.

All 4,280 affected cases passed with 38 optional skips, plus 375 repeated race
cases, 40 API cases and both builds. The clean integration passed
9,942 cases with 38 optional skips across 106 packages
and built at the same commit. Actual tests separately verify registered API/PIN
boundaries, byte-identical extracted H264/mono and HEVC/stereo files, and a real
local TCP transfer aborted after an authority change. Independent review verified
all ten source files (five production), patch/archive, binary, runners and raw
reports. The one-MiB synthetic handler measurement is 70 repository reads,
reduced from 312; it is not a throughput guarantee. Closed-file size/mtime checks
are not proof against a privileged in-place rewrite. Cancellation closes inputs
and retains the slot until writer cleanup joins; arbitrary writers are not
claimed to interrupt instantly. No new age/provider permission is inferred.
[Exact ten-file delta](https://github.com/stefnats/mediastorm/compare/0892cd709fdf9fc7d8dcaec69d3595d5ba2027a0...399c7f10aefa9b48f970aa6182f9b672c88f79df).

The first full integration run at the export commit retained one failure in an
existing channel-filter fixture: a recycled loopback URL reused another test's
persistent playlist cache. Its original 9,941 passes, one failure and raw cache-hit
log remain preserved. A separate test-only live fixture correction gives each
synthetic M3U URL a unique query nonce and asserts actual initial fixture fetches.
No production cache is read, deleted or changed by that correction. The final
full-suite and clean-build figures above apply to integration commit
[0c103f37e55e4766850f070617b497eb2bc82664](https://github.com/stefnats/mediastorm/commit/0c103f37e55e4766850f070617b497eb2bc82664), including
this isolated fixture fix; they do not recast the first failing run as green.

## Fork-only original menu and Set Aside mutation context

The [native selection correction](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-setaside-context)
captures the original target, epoch and producer lifetime before deferred menus,
pickers or sheet animations. A synchronous one-use acceptance allows intentional
menu dismissal; cancelled hiding or retired producers cannot start stale writes.
Accepted work is a real child of the original producer, so another blocked child
cannot postpone its cancellation. Reserved Set Aside discovery/create/mutation
and refresh are serialized per original owner, including strict fresh discovery
and validation of the returned reserved list. Replacement UI, errors and toasts
remain guarded. Existing Watchlist/Hide/Watched API workflows are not declared
fully migrated by this packet.

All 1,743 native cases passed, including
42 new tests, and both APKs built. Frozen review verifies
all 28 files (23 production),
raw XML, APK hashes, registry and five empty private prefills. Tests execute the
actual receipt/workflow/Store publisher, phone menu singleton and loopback
Call.await cancellation. Complete successful Android ApiClient.send and Compose
sheet/navigation execution remain source/compile-reviewed. One changed helper
currently has no caller and is not presented as an exercised producer. Source
review findings were corrected before final runs; no executed pre-fix failure is
invented. A preliminary green run was repeated after adding an explicit test
sibling-entry barrier, with its original reports retained.
[Exact native delta](https://github.com/stefnats/mediastorm-androidtv/compare/66f2255302ed9b86e63f25d6d41b067b02f41915...0cbb77e09e6f01a2080942a9ed11475a727c9ffe).

Both packets remain in the forks. No production activation, actual provider,
private playlist publication or new upstream submission occurred.

## Fork-only indirect native page ownership

The [four-file page correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-indirect-pages)
retains the original epoch through merged service children, Continue Watching
and its optional Set Aside lookups, library queries and hidden-item page reads.
Merged totals cannot cross owners or accept late writes. Hidden pages consume
their own immutable response, without adopting a shared background refresh's
singleton value. A common read generation protects publication order.

Direct page reads propagate cancellation and terminal denials. The explicitly
separate existing background refresh retains its original logging/no-publication
failure policy; it does not produce a successful empty page. This avoids adding
uncaught terminal exceptions to unrelated legacy UI callers. An initial automatic
review rejected an unqualified rollback; no rejected patch ran. The approved
scope clarification and separate actual contracts preserve the strict page path.

All 1,778 native cases passed, including 35 new cases, and both APKs built.
Independent frozen review and integration verification checked all seven sources
(four production), full patch, raw XML, APK hashes, registry and five empty private
prefills. Tests consume actual page adapters, merge cache, Store, late-page model
and request helpers, plus loopback Call.await. Complete successful Android
ApiClient.send, real profile activation and device/Compose behavior remain
source/compile-reviewed. An intermediate Android logger harness failure was
observed in tool output, but its raw log/XML were overwritten before archival;
no missing source archive or production RED is invented.

Other direct Home/ListsTab/Launcher/library-opener consumers and legacy
Watchlist/Hide/Watched mutations remain distinct follow-ups. There is no all-Home,
all-writers, media authorization, age grant or activation claim.
[Exact seven-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/0cbb77e09e6f01a2080942a9ed11475a727c9ffe...07bf51502e7c46a3ed4f58bd51346d5a4928efed).

The packet remains in the fork; no production activation, private playlist or
new upstream submission occurred.

## Fork-only protected web DVR downloads

The [recordings UI](https://github.com/stefnats/mediastorm/tree/feature/web-dvr-export)
now offers original-parts ZIP downloads in admin and account views. Large archives
use the browser's normal download transport, without a full in-memory Blob.
Temporary owner unlocks travel only in a bounded same-origin POST body, never
in URLs, cookies or persistent client storage. The private cookie bridge checks
current full-session/account/profile authority, exact Origin and a single token;
PIN replies are buffered and released only after unchanged original authority.
No public API/export-core behavior or age policy is changed.

Deferred frame/PIN replies retain their action generation. An old denial cannot
replace a newer prompt or start a download after a profile change. Status says
requested, not completed; browser cancellation and the original export guard
control actual transfer lifetime. Two actual candidate REDs are archived with
source/test hashes: stale A/B control and an authorized comma-token collection
that violated the new single-value protocol, not an authentication bypass.
The A/B case is unchanged in the final Python although a separate later block
was added; whole-file identity is not invented.

All 4,318 affected normal cases passed with 39 optional skips; 111 main-package
race cases, 40 API cases, three actual browser flows and their nine repeated race
runs passed. The real recordings page downloaded six byte-identical ZIPs across
admin/account and 320/390/1280 widths, without URL credentials or page errors.
Actual tests cover late PIN/profile replies, cancellation/slot join and PIN
replacement during a transfer. Independent review verified all seven files
(three production), patch/archive, binary, runners, raw reports and RED snapshots.
A historical rate-limit assertion crossed a real refill interval, not a data race;
its corrected burst test is separate from actual wrong-PIN verification. One
existing planning selector now targets its forecast aria-label; the unchanged
assertion and all three final browser flows were rerun after that test-only edit.

The clean integration commit
[79005bb6e18f3f4a69a7f1063dd3c791fa7c4526](https://github.com/stefnats/mediastorm/commit/79005bb6e18f3f4a69a7f1063dd3c791fa7c4526)
retains the isolated live fixture correction and passes 9,980
cases with 38 optional skips across 106 packages,
including the actual new browser test, then builds at the same clean commit.
[Exact seven-file feature delta](https://github.com/stefnats/mediastorm/compare/399c7f10aefa9b48f970aa6182f9b672c88f79df...8d90049df7edc26d1522c56e5714aeb149f74036).
No production activation, real provider, private playlist or new upstream
submission occurred. Physical Safari/native-device acceptance remains separate.

## Fork-only original home row lifetime

The [home ownership correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-home-context)
keeps home settings, rows, previews, live/hub feeds and deferred work bound to the
original epoch and producer. Row supervision is a real child of its caller;
mobile entries and TV state retire by epoch rather than profile ID alone. The
three narrow screen/state carriers restart the correct store after retirement.
New generations cancel old prefills and completions. Known terminal denial
retires remaining jobs and visible rows, while an explicit new full load can
reauthorize. Automatic partial/staleness work cannot revive a denied run.

Legitimate progressive COMPLETE passes and calendar retries remain alive after
the first card pass. Actual positive Store tests verify both. A separate actual
scheduling RED showed COMPLETE starting after a known shared-feed denial; its
full sixteen-file candidate source/patch/manifest and raw XML/log/exit are kept.
The ordering fix validates already requested shared data before followups in
both All and non-All views, without starting a disabled feed. Another actual
Store test holds an accepted COMPLETE noncancellably, introduces partial denial,
then verifies the late answer cannot republish rows or trigger automatic work.

All 1,815 native cases passed, including 37 new cases, and both APKs built.
Independent review and root verification checked all sixteen files (eleven
production), full patch, 229 JUnit files, APK hashes/DEX symbols, registry and five
empty private prefills. Tests execute the actual Store with narrow IO/platform
ports, shared context, row adapters, caches and entry/state factories. Compose
recomposition, complete successful Android ApiClient.send, device activation and
Launcher transport remain source/compile-reviewed or explicitly outside this
packet. Runner hashes for the historical RED were pinned after it started;
pre-start pinning is not claimed. Earlier preliminary runs remain historical.

This is row-work ownership through the Launcher handoff, not an all-Launcher,
all-ListsTab, all-writers or age/parent authorization migration.
[Exact sixteen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/07bf51502e7c46a3ed4f58bd51346d5a4928efed...fb27be664f33e89283fd48553f34de6bc14fdd94).
No production activation, private playlist or new upstream submission occurred.

## Fork-only hidden management and original hide context

The [backend Hidden management source](https://github.com/stefnats/mediastorm/tree/feature/hidden-catalogue-language)
selects original languages from the complete owned saved index before totals and
pagination. It retains raw HiddenItem fields and reads only existing validated
catalogue evidence. Unknown can be selected alone or together with registered
ISO 639-3 codes. Unsupported metadata-dependent filters reject before demand.
There is no discovery-window cap, provider enrichment or suppression-set rewrite.
The global Hidden store must remain the complete suppression set; a later native
management consumer must keep filtered pages separate. Ten actual handler replies
and exact-base legacy replay are retained. Legacy evidence covers the observed
unsupported-source family, not arbitrary old servers.

All 4,542 focused normal cases, 423 race pass events and 40 API cases passed;
41 normal cases explicitly skipped optional fixtures. Independent review checked
seven changed sources, three production, archive, patch, binary and raw reports.
The combined clean backend at 86d144bf186ff4f95b32011ba25d5050034c9a44 passed
10,036 cases across 106 packages with 38 optional skips, then built at that same
revision. Existing cache housekeeping may create directories or remove expired
entries; literal filesystem-read-only behavior is not claimed.

The separate [native hide ownership correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-hide-context)
keeps hide, unhide, continue-watching removal and alias deletes attached to their
original owner, target and producer. Store revisions protect accepted mutations
from old refreshes. Original route predicates prevent a late operation dismissing
replacement Details or showing a stale success. A server-accepted write may remain
after cancellation; there is no replacement rollback. Watchlist, Watched and
Launcher writers remain separate followups.

All 1,836 native cases passed, including 21 new cases, and both APKs built.
Independent review and root checks cover sixteen sources, thirteen production,
232 JUnit reports, APK/DEX checks, registry and five private empty prefills.
Actual Store, origin, mutation and cancellation helpers execute under tests;
successful complete Android ApiClient.send, Compose route transitions, touch and
physical devices remain outside this evidence. No executed pre-fix RED is claimed.

[Backend delta](https://github.com/stefnats/mediastorm/compare/79005bb6e18f3f4a69a7f1063dd3c791fa7c4526...86d144bf186ff4f95b32011ba25d5050034c9a44).
[Native delta](https://github.com/stefnats/mediastorm-androidtv/compare/fb27be664f33e89283fd48553f34de6bc14fdd94...a9b360c3670453a656ee0b764bee44c966b1bccd).
No production activation, private playlist or new upstream submission occurred.

## Fork-only complete local-library original-language cards

The [local-library card filter](https://github.com/stefnats/mediastorm/tree/feature/local-library-language)
selects complete saved local groups before totals and pagination. It retains all
versions of a selected group. A consistent stored catalogue assignment across all
members is required, including members hidden by other filters. Missing or
conflicting assignments stay Unknown. Manual assignments name a catalogue work;
they do not prove a file, audio track, age or playback permission. Existing strict
cached provider evidence supplies original language without warming a cache miss.

The same local include=cards response advertises the boolean capability. Active
queries on remote libraries or detail projections reject before listing or demand.
Filtered cards skip episode enrichment and artwork localization. No extra probe,
scan, file open or provider request is introduced. Real synthetic PostgreSQL tests
seed 1,304 versions, reach a matching group beyond index 650, preserve the complete
stored index and exercise concurrent views. A poisoned remote table verifies that
filtered remote rejection occurs before remote listing. Separate actual API
ownership and wire/schema fixtures have explicitly narrower claims. Eight actual
serialized Go replies are retained for the native consumer.

All 4,683 focused normal cases passed with 41 explicit optional skips, alongside
207 race pass events, 41 API cases and both builds. Root and independent review
verified ten sources, four production, source archive, binary, runners and raw
reports. The first combined run remains recorded as 10,102 pass and one existing
station pagination failure with playlist cache hits; it is not reported as green.

The separate [test fixture correction](https://github.com/stefnats/mediastorm/tree/test/playlist-fixture-isolation)
isolates eight existing test files using the existing URL nonce helper and real
first-fetch counters. Warm reuse and all access/revocation/grouping/media assertions
remain. No production source or cache contents were read, scanned or removed.
Only the grouped-pagination failure was executed; the other affected fixtures
were identified from source. The unchanged final test delta passed 315 repeated
normal and 189 race events, plus both real offline/silent failover variants in
normal and race runs. Existing API coverage is included, not counted twice.

Only the corrected clean combined commit 81811752a0543fc5debecdef6acaadeb633bb20b passed 10,103
cases across 106 packages with 38 optional skips,
then built at the same revision. Original-language catalogue filtering does not
prove spoken audio or complete remote metadata. The separate native management
consumer remains in progress. No deployment, private playlist, real provider or
new upstream submission occurred.

## Fork-only native management language filters

The [native management consumer](https://github.com/stefnats/mediastorm-androidtv/tree/feature/native-management-language)
adds original-language controls to Hidden management and local-library cards.
Hidden pages use a separate strict raw management reader and never replace the
complete global Hidden suppression store. A row hint offers Unhide even before
the global store loads, using the existing original operation receipt. Local
library support comes from the same normal include=cards response. Explicit
remote sources remain unsupported; an unknown source is not assumed local.

A strict active/supported page cannot silently drop malformed rows or retain
older filtered pages after a bad continuation. Totals remain group totals;
itemCount remains the raw version count rather than a new displayed UI field.
Replaced read children are cancelled before they could start a legacy followup.
Only the observed initial default/unfiltered exact unsupported-source 400 allows
an owned full legacy Hidden read, without global Store publication. Active
filters, sorted/kind requests, authentication, 503, network, malformed responses
and later supported-view downgrades never take that fallback.

All 1,867 native cases passed, including 31 new cases, and both APKs built.
Independent and root review verify 36 changed sources, 11 production, 236 raw XML
reports, pre-run source/runner pins, APK/DEX symbols, registry and five empty
private prefills. The first full run remains archived as 1,660 core pass and one
obsolete baseline expectation rejecting Hidden; TV and phone did not run there.
Its complete 35-file candidate tree, raw core reports and separately pinned
unchanged committed-base test are retained. Only that existing test expectation
changed; the 11 production hashes stayed identical before the successful rerun.

Eighteen actual Go handler replies are byte-identical in native fixtures. Six
requests exported by the production native builders pass the separate
[Go reverse contract](https://github.com/stefnats/mediastorm/tree/test/native-management-contract):
7 normal and 21 race pass events with no failures/skips, through actual Go
handlers, cached evidence readers and the OpenAPI schema. Service-port wire
fixtures do not claim PostgreSQL, network login or Android transport. Native
models/decoders and a separate two-request context-bound loopback HTTP helper run;
full successful authenticated Android ApiClient.send, Compose effects, physical
input and device playback remain unverified. No original-language audio proof,
provider probe, new media/parent grant or all-catalogue filter is introduced.

[Exact native delta](https://github.com/stefnats/mediastorm-androidtv/compare/a9b360c3670453a656ee0b764bee44c966b1bccd...4a2ad7eafb70a5d5f4e37c8e96ce4c0f65f27400).
The reverse test is committed separately at 14154ed7da23d0f87be0ffb9ed13666d5b2ec197;
its merge into the next combined backend run is pending. No production activation,
private playlist or new upstream submission occurred.

## Fork-only durable SAB download handoff status

The [SAB fallback status packet](https://github.com/stefnats/mediastorm/tree/feature/sab-fallback-status)
makes the existing explicitly configured final-import-failure handoff durable.
A private receipt is stored before submission. Lost acknowledgements and crash
windows remain submission_unknown; retry/restart never automatically resubmits.
Concurrent metadata replacement or re-adding a queue item preserves its receipt.
The endpoint/credential fingerprint prevents querying an old job on a replacement
backend. A fresh source check follows a blocked refresh CAS before any status HTTP.
Only bounded regular no-follow NZB descriptors are read, with a 32 MiB limit.
This is not an immutable-file or instant disk-read cancellation guarantee.

GET returns persisted sanitized state only. POST explicitly refreshes status with
bounded concurrency and the original request/session/account deadline capped at
five seconds. Both actual current SQL master role and nonrenewing session snapshot
are checked before and after work, without trusting cached IsMaster. Main uses
the same two-route registration in actual cookie/origin loopback tests. Output
contains only queue ID, allowlisted state/reason and observation times, without
credentials, raw provider errors, paths, URLs or private SAB job identifiers.
download_completed never changes the failed import into a playable or imported
item. No new automatic Play, Sonarr/Radarr, target storage, quota, cleanup or
media/parent permission is introduced.

All 4,697 focused normal cases passed with 39 explicit optional skips, alongside
303 race events, 42 API cases and both builds. Actual isolated PostgreSQL role
and session tests and actual registered cookie/origin cases each passed eight
normal and 24 race events, overlapping those totals. Root's earlier 42/126
session/account-port reports precede the final entry-deadline guard; final author
runs bind those same test cases to the final source. Seven actual sanitized wire
replies and schema provenance are retained. Independent and root verification
cover all twelve sources, six production, complete patch/archive, binary,
pre-run source pin, runners, tools and raw reports.

Two executed regressions retain exact source archives: ten non-object re-add
cases erased receipts, and a changed-backend refresh after a blocked SQLite CAS
issued one old status request instead of zero. An earlier CAS fixture counter
failure is recorded separately rather than called a product RED. The final test
captures the old configuration after the actual copy under its lock.

The clean combined commit 65509d379e6007c914aff1bfe691b443e373f9c0 passed 10,212 cases across
106 packages with 38 optional skips, including the
previously separate native management reverse contract. Runner, source worktree
and frozen native query bytes stayed unchanged. The server then built at that
same revision. Native device execution and a complete managed download/import
workflow remain separate. No production activation, private playlist, real
provider request or new upstream submission occurred.

## Fork-only original Watchlist mutation context

The [Watchlist mutation correction](https://github.com/stefnats/mediastorm-androidtv/tree/fix/native-watchlist-context)
binds all seven inventoried writers to their original immutable title, owner,
producer and displayed desired state before deferred work. Existing operation
receipts remain unchanged. The actual HomeScreen/stacked carousel carriers and
TV/phone Details page predicates retire old actions; retained visible returns
receive fresh operation and busy generations. PostPlay uses the original player
context and actual suggestion instance, retiring its list work on replacement.

Owner-serialized mutations and read revisions prevent old refreshes or rollbacks
from replacing newer state. Alias overlays are retired by later overlapping
accepted mutations. Owned readback rejects malformed or partial rows and totals.
It is still only the complete visible display projection: Hidden and profile
unreleased rules can legitimately omit a saved title. Absence never disproves an
accepted save and overrides never insert hidden rows into the visible projection.

A confirmed successful write retains its desired local intent when subsequent
nonterminal refresh fails, with an explicit saved-but-refresh-failed error and
no fresh-row/loaded success. Pre-write failure rolls back; cancellation or terminal
readback publishes no late confirmation/success. Already accepted server writes
may remain. The existing background refresh keeps its actual caller contract:
a failed attempt finishes with a visible static error and unchanged rows, rather
than an endless loading state or successful empty result. Direct owned reads
remain strict. Normal menu dismissal still allows the accepted action's bounded
error notification without touching a replacement page.

All 1,904 native cases passed, including 37 new cases, and both APKs built.
Independent and root review verify nineteen sources, fourteen production, all
240 JUnit reports, pre-run source/runner pins, APK/DEX symbols, registry and five
empty private prefills. Actual Store, Editor, menu/operation adapters, API entry
cancellation and Call.await loopback boundaries execute. Full successful Android
ApiClient.send, Compose transitions, physical touch, device playback and complete
Launcher/Watched/ratings transport coverage remain separate. Review source points
were corrected before final tests; no executed production RED is claimed.
Preliminary test compilation errors retain separate logs. Existing PostPlay test
assertions remain; the fake signature and two positive inputs now use the actual
suggestion instance required by the existing UI path.

[Exact nineteen-file delta](https://github.com/stefnats/mediastorm-androidtv/compare/4a2ad7eafb70a5d5f4e37c8e96ce4c0f65f27400...dfcf4ed12cd782b444adcf5ca0702453ba829561).
No production activation, private playlist or new upstream submission occurred.
