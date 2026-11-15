# Active corrections

All three source heads remain unchanged. Their common upstream base is `8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0`. There is no required merge order and no deferred feature dependency. Test counts include subtests where recorded.

## Backend 82: test lifecycle synchronization

[PR](https://github.com/godver3/mediastorm/pull/82) | [Pinned diff](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...f24fa1b9c82d23544401a160752f7a9dc81ac210) | Head `f24fa1b9c82d23544401a160752f7a9dc81ac210`

The forced-HLS test shuts down its background manager before temporary storage is removed. History tests wait for stale-progress cleanup. Torrent preflight allows the losing request to be cancelled before reaching the test server, while retaining the enriched-metadata and observed-download-count assertions.

Review `backend/handlers/video_live_usage_test.go`, `backend/services/history/service_test.go` and `backend/services/debrid/torrent_preflight_test.go`. They are the entire diff: 30 additions and four deletions. Production code is unchanged. The corrections from closed PRs 94 and 95 are included here.

The complete `handlers`, `services/history` and `services/debrid` packages passed 3,282 cases with seven skips on this head. [Test evidence](test-reliability.json) and [branch verification](isolated-fixes.json) record that run.

## Backend 81: expire guest media authority with the room

[PR](https://github.com/godver3/mediastorm/pull/81) | [Pinned diff](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...72c4e347da9a49a75ff5fd6ec8eda88a09277712) | Head `72c4e347da9a49a75ff5fd6ec8eda88a09277712`

A guest grant is bound to its room, membership and media resource. Ending or expiring a room, removing the guest or changing the source prevents fresh playback through that grant. Paused rooms and ordinary shares retain their behavior.

Start with `backend/services/sessions/watch_party.go`, then `backend/handlers/watch_party_grant_lifecycle_test.go`. `backend/services/watchrooms/service.go` supplies the room and membership authority check. The eight-file diff uses the existing restricted stream scope and needs no migration.

The complete session and watch-room service packages passed 56 cases, and the actual grant endpoint passed one isolated lifecycle regression. [Branch verification](isolated-fixes.json) records these checks. Earlier browser checks used a combined application and are historical integration evidence, not isolated acceptance of this branch.

Already buffered media cannot be revoked. Legacy unbound grants retain their original lifetime unless revoked; new room-bound grants need to be issued for the binding to apply. Downgrading does not unwrap the new binding.

## Backend 96: keep warmed preparation owned by the requesting profile

[PR](https://github.com/godver3/mediastorm/pull/96) | [Pinned diff](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...2f772452ab0bad47afda45fb41f6887d8bb1a887) | Head `2f772452ab0bad47afda45fb41f6887d8bb1a887`

A global warm reference may belong to another account or a locked sibling profile, so returning its preparation ID causes a later poll to fail. The requesting profile now reuses its own ready or pending entry, or starts normal preparation under its own settings and permissions.

The production diff is in `backend/handlers/prequeue.go`: five additions and nine deletions. The existing title/profile/settings index is checked before foreign source validation, deletion or adoption. `backend/handlers/prequeue_profile_warm_test.go` contains the regression coverage and is the only other changed file.

The complete playback and prewarm service packages passed 195 cases. The five warm-reference handler tests passed seven cases including subtests. [Branch verification](isolated-fixes.json) records this isolated run; earlier repeated race and browser checks remain historical evidence on the same source head.

A new profile may require its own first preparation. Existing source/search caches still apply. The change needs no migration or client update.
