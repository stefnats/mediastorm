# Deferred proposals

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
