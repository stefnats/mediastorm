# Deferred proposals

PRs [81](https://github.com/godver3/mediastorm/pull/81), [82](https://github.com/godver3/mediastorm/pull/82) and [96](https://github.com/godver3/mediastorm/pull/96) were merged by the maintainer on 15 November 2026 at 19:30 Europe/Berlin. No own upstream PR is open in either repository. Deferred features remain in the forks pending scope discussion. Successive fork-only entries below retain the review status at the time of each package.

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
