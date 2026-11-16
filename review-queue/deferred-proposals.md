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
