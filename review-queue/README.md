# Upstream review queue

The active queue contains three independent backend changes based directly on upstream master. Feature proposals are deferred pending a scope discussion with the maintainer. Further feature work stays in the fork until a review scope is agreed.

| Active PR | Scope | Review size |
| --- | --- | --- |
| [Backend 81](https://github.com/godver3/mediastorm/pull/81) | Expire watch-room guest media authority with the room | 8 files; one focused fix |
| [Backend 96](https://github.com/godver3/mediastorm/pull/96) | Keep warmed playback references bound to the requesting profile | 2 files; one focused fix |
| [Backend 82](https://github.com/godver3/mediastorm/pull/82) | Stabilize asynchronous lifecycle tests | 3 test files; 30 additions, 4 deletions |

None of these requests requires a deferred feature PR. Backend 82 combines its original HLS manager cleanup with the corrections previously proposed in backend 94 and 95. Production code is unchanged in that request. The two security/correctness fixes retain their original source heads.

## Deferred topics

The source branches and original test evidence remain available. Grouping below is a planning aid, not a request to merge a combined feature branch. Each future submission needs an agreed scope, an isolated diff against then-current upstream and fresh validation.

| Topic | Previous backend PRs | Previous native PRs |
| --- | --- | --- |
| Provider capacity, station fallback and recorder lifecycle | 83, 84, 93 | 18 |
| Local source choice, release reports and replacement playback | 85, 86, 87 | 4, 5 |
| Source availability, visible titles/episodes and explicit rechecks | 79, 88, 89, 90, 91, 92 | 6, 7, 8, 9, 10 |
| Recommendations, personal ratings and taste setup | 80, 98, 99 | 11, 12 |
| Personal lists and request/profile isolation | 100 | 13, 14, 17 |
| Episode autoplay | 101 | 15 |
| Completion choices | 102 | 16 |
| Combined integration evidence | 97 | None |

## Preserved source and validation

- [mediastorm: preserved combined source](https://github.com/stefnats/mediastorm/tree/43fae17782b56175b35c0d3668b1852a8820dd21), also tagged `archive/review-2026-11-15`. All originally open heads in this repository are ancestors of this source.
- [mediastorm-androidtv: preserved combined source](https://github.com/stefnats/mediastorm-androidtv/tree/660a9de6bd3c65f08d9c96b296c3d1fb28036083), also tagged `archive/review-2026-11-15`. All originally open heads in this repository are ancestors of this source.

The last combined backend verification passed 8,204 Go cases at the preserved backend product source. The latest native verification passed 882 tests and built both APKs. These are historical checks of those exact feature trees; they do not replace isolated validation for future proposals. Native device acceptance remains pending. No upstream merge or production deployment is part of this reorganization.

[Test-package validation](test-reliability.json), [isolated checks of all three review branches](isolated-fixes.json) and [preservation checks for every original head](original-pr-preservation.json) are recorded alongside this plan.

## Original requests and branches

Every original branch is retained. Only backend 82 advances, by a fast-forward containing the other two test corrections. The original SHA below remains in its history. No source branch is deleted or force-pushed.

| Repository / original PR | Disposition | Preserved original head | Fork branch |
| --- | --- | --- | --- |
| [mediastorm 79](https://github.com/godver3/mediastorm/pull/79) | Deferred | [`45184cbdbfda`](https://github.com/stefnats/mediastorm/commit/45184cbdbfda3dad3670dd3b030a382de25a8fe2) | [codex/source-availability-overviews](https://github.com/stefnats/mediastorm/tree/codex/source-availability-overviews) |
| [mediastorm 80](https://github.com/godver3/mediastorm/pull/80) | Deferred | [`68db2b17d7b2`](https://github.com/stefnats/mediastorm/commit/68db2b17d7b2147b2513306d80afd692fe02a06f) | [codex/recommendation-interest-seeds](https://github.com/stefnats/mediastorm/tree/codex/recommendation-interest-seeds) |
| [mediastorm 81](https://github.com/godver3/mediastorm/pull/81) | Active | [`72c4e347da9a`](https://github.com/stefnats/mediastorm/commit/72c4e347da9a49a75ff5fd6ec8eda88a09277712) | [codex/watchparty-media-lifecycle](https://github.com/stefnats/mediastorm/tree/codex/watchparty-media-lifecycle) |
| [mediastorm 82](https://github.com/godver3/mediastorm/pull/82) | Active | [`150d831128f6`](https://github.com/stefnats/mediastorm/commit/150d831128f668152b7d1004570456678505d285) | [codex/live-hls-test-cleanup](https://github.com/stefnats/mediastorm/tree/codex/live-hls-test-cleanup) |
| [mediastorm 83](https://github.com/godver3/mediastorm/pull/83) | Deferred | [`a1e660830c98`](https://github.com/stefnats/mediastorm/commit/a1e660830c98d5b6edb1d524d4f0c913bc39bc56) | [codex/iptv-capacity](https://github.com/stefnats/mediastorm/tree/codex/iptv-capacity) |
| [mediastorm 84](https://github.com/godver3/mediastorm/pull/84) | Deferred | [`fbf83ade63b6`](https://github.com/stefnats/mediastorm/commit/fbf83ade63b6461b817f134e879e74e57cf65ee8) | [codex/live-station-failover](https://github.com/stefnats/mediastorm/tree/codex/live-station-failover) |
| [mediastorm 85](https://github.com/godver3/mediastorm/pull/85) | Deferred | [`aaac2269109a`](https://github.com/stefnats/mediastorm/commit/aaac2269109a634f7fd2db7cba250d45d238e624) | [codex/local-playback-fallback](https://github.com/stefnats/mediastorm/tree/codex/local-playback-fallback) |
| [mediastorm 86](https://github.com/godver3/mediastorm/pull/86) | Deferred | [`708fbb673278`](https://github.com/stefnats/mediastorm/commit/708fbb67327835d1810d4ca7b05dbc1da0600ab3) | [codex/scoped-release-reports](https://github.com/stefnats/mediastorm/tree/codex/scoped-release-reports) |
| [mediastorm 87](https://github.com/godver3/mediastorm/pull/87) | Deferred | [`4675137e7ad7`](https://github.com/stefnats/mediastorm/commit/4675137e7ad71d6df556b3589c8d5523273df6ef) | [codex/web-prepared-replacements](https://github.com/stefnats/mediastorm/tree/codex/web-prepared-replacements) |
| [mediastorm 88](https://github.com/godver3/mediastorm/pull/88) | Deferred | [`bf35f120b4cd`](https://github.com/stefnats/mediastorm/commit/bf35f120b4cd82f299cd18ce214209e923cf4193) | [codex/scoped-source-availability](https://github.com/stefnats/mediastorm/tree/codex/scoped-source-availability) |
| [mediastorm 89](https://github.com/godver3/mediastorm/pull/89) | Deferred | [`dcf0673c1e20`](https://github.com/stefnats/mediastorm/commit/dcf0673c1e20e3179ae10862b17c44165beee874) | [codex/episode-source-hints](https://github.com/stefnats/mediastorm/tree/codex/episode-source-hints) |
| [mediastorm 90](https://github.com/godver3/mediastorm/pull/90) | Deferred | [`2faa5be6c249`](https://github.com/stefnats/mediastorm/commit/2faa5be6c24973379af2320bb94549ace6fac343) | [codex/visible-title-source-hints](https://github.com/stefnats/mediastorm/tree/codex/visible-title-source-hints) |
| [mediastorm 91](https://github.com/godver3/mediastorm/pull/91) | Deferred | [`3e40ff54fb2a`](https://github.com/stefnats/mediastorm/commit/3e40ff54fb2a3a56525a6a4520f293a0fb29b8ed) | [codex/local-source-hints](https://github.com/stefnats/mediastorm/tree/codex/local-source-hints) |
| [mediastorm 92](https://github.com/godver3/mediastorm/pull/92) | Deferred | [`a5427aa085b7`](https://github.com/stefnats/mediastorm/commit/a5427aa085b7327da66fa3f66a2857ca74baefef) | [codex/manual-source-recheck](https://github.com/stefnats/mediastorm/tree/codex/manual-source-recheck) |
| [mediastorm 93](https://github.com/godver3/mediastorm/pull/93) | Deferred | [`d9464549ee7d`](https://github.com/stefnats/mediastorm/commit/d9464549ee7d9dfc789cc0002d6fd070fc5b6668) | [codex/recording-stop-cause](https://github.com/stefnats/mediastorm/tree/codex/recording-stop-cause) |
| [mediastorm 94](https://github.com/godver3/mediastorm/pull/94) | Folded into backend 82 | [`3f01ff3533ea`](https://github.com/stefnats/mediastorm/commit/3f01ff3533ea1467cb9461905fe17616d59d65d7) | [codex/history-cleanup-test](https://github.com/stefnats/mediastorm/tree/codex/history-cleanup-test) |
| [mediastorm 95](https://github.com/godver3/mediastorm/pull/95) | Folded into backend 82 | [`86bcc28cdeae`](https://github.com/stefnats/mediastorm/commit/86bcc28cdeae71af03e86d67e2af6ff6d9cb1f0b) | [codex/debrid-race-test](https://github.com/stefnats/mediastorm/tree/codex/debrid-race-test) |
| [mediastorm 96](https://github.com/godver3/mediastorm/pull/96) | Active | [`2f772452ab0b`](https://github.com/stefnats/mediastorm/commit/2f772452ab0bad47afda45fb41f6887d8bb1a887) | [codex/profile-bound-prewarm](https://github.com/stefnats/mediastorm/tree/codex/profile-bound-prewarm) |
| [mediastorm 97](https://github.com/godver3/mediastorm/pull/97) | Deferred | [`52edf11fe62c`](https://github.com/stefnats/mediastorm/commit/52edf11fe62c7ade82de56834d0b7bcccc25d237) | [codex/family-source-integration](https://github.com/stefnats/mediastorm/tree/codex/family-source-integration) |
| [mediastorm 98](https://github.com/godver3/mediastorm/pull/98) | Deferred | [`57e5f8d019d9`](https://github.com/stefnats/mediastorm/commit/57e5f8d019d9b1913f0f05f88136af02d76f31ae) | [codex/profile-title-ratings](https://github.com/stefnats/mediastorm/tree/codex/profile-title-ratings) |
| [mediastorm 99](https://github.com/godver3/mediastorm/pull/99) | Deferred | [`b98fa90a3f3e`](https://github.com/stefnats/mediastorm/commit/b98fa90a3f3e23e3873592c7e086b041a5c861d8) | [codex/profile-taste-onboarding](https://github.com/stefnats/mediastorm/tree/codex/profile-taste-onboarding) |
| [mediastorm 100](https://github.com/godver3/mediastorm/pull/100) | Deferred | [`28442cf7eb13`](https://github.com/stefnats/mediastorm/commit/28442cf7eb13d0e2e5aecdd3f0946de741219646) | [codex/profile-personal-area](https://github.com/stefnats/mediastorm/tree/codex/profile-personal-area) |
| [mediastorm 101](https://github.com/godver3/mediastorm/pull/101) | Deferred | [`26d4987b372c`](https://github.com/stefnats/mediastorm/commit/26d4987b372c09ef2f73d450851f1bd08e68020a) | [codex/profile-episode-autoplay](https://github.com/stefnats/mediastorm/tree/codex/profile-episode-autoplay) |
| [mediastorm 102](https://github.com/godver3/mediastorm/pull/102) | Deferred | [`43fae17782b5`](https://github.com/stefnats/mediastorm/commit/43fae17782b56175b35c0d3668b1852a8820dd21) | [codex/profile-post-play](https://github.com/stefnats/mediastorm/tree/codex/profile-post-play) |
| [mediastorm-androidtv 4](https://github.com/godver3/mediastorm-androidtv/pull/4) | Deferred | [`f1eedc7d9b00`](https://github.com/stefnats/mediastorm-androidtv/commit/f1eedc7d9b0097a5d380dad1817e13766b1feb54) | [codex/native-release-reports](https://github.com/stefnats/mediastorm-androidtv/tree/codex/native-release-reports) |
| [mediastorm-androidtv 5](https://github.com/godver3/mediastorm-androidtv/pull/5) | Deferred | [`0255a8b7f8f6`](https://github.com/stefnats/mediastorm-androidtv/commit/0255a8b7f8f6d822a92f8c03e8350597d4134617) | [codex/audio-recovery-suggestions](https://github.com/stefnats/mediastorm-androidtv/tree/codex/audio-recovery-suggestions) |
| [mediastorm-androidtv 6](https://github.com/godver3/mediastorm-androidtv/pull/6) | Deferred | [`b854f2b08389`](https://github.com/stefnats/mediastorm-androidtv/commit/b854f2b0838975b0053a0bfc3a59ef9f8c0418db) | [codex/native-source-hints](https://github.com/stefnats/mediastorm-androidtv/tree/codex/native-source-hints) |
| [mediastorm-androidtv 7](https://github.com/godver3/mediastorm-androidtv/pull/7) | Deferred | [`31815127c374`](https://github.com/stefnats/mediastorm-androidtv/commit/31815127c37423a27a14c0879453e79a4373ba70) | [codex/visible-episode-hints](https://github.com/stefnats/mediastorm-androidtv/tree/codex/visible-episode-hints) |
| [mediastorm-androidtv 8](https://github.com/godver3/mediastorm-androidtv/pull/8) | Deferred | [`8295e8370b81`](https://github.com/stefnats/mediastorm-androidtv/commit/8295e8370b812978cf4aeb12ba749acb6f42e7ae) | [codex/visible-title-hints](https://github.com/stefnats/mediastorm-androidtv/tree/codex/visible-title-hints) |
| [mediastorm-androidtv 9](https://github.com/godver3/mediastorm-androidtv/pull/9) | Deferred | [`22ce3ef8f68e`](https://github.com/stefnats/mediastorm-androidtv/commit/22ce3ef8f68ea7349aada4cb4c37ca6c0b3cd821) | [codex/local-source-labels](https://github.com/stefnats/mediastorm-androidtv/tree/codex/local-source-labels) |
| [mediastorm-androidtv 10](https://github.com/godver3/mediastorm-androidtv/pull/10) | Deferred | [`4188f9c69830`](https://github.com/stefnats/mediastorm-androidtv/commit/4188f9c6983018cb7af5f8fc51192d9c26d11453) | [codex/source-recheck-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/source-recheck-controls) |
| [mediastorm-androidtv 11](https://github.com/godver3/mediastorm-androidtv/pull/11) | Deferred | [`59fe2d5c7433`](https://github.com/stefnats/mediastorm-androidtv/commit/59fe2d5c743369f2d0db7dd7b30e8f5c554ca513) | [codex/profile-rating-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-rating-controls) |
| [mediastorm-androidtv 12](https://github.com/godver3/mediastorm-androidtv/pull/12) | Deferred | [`845471ca931d`](https://github.com/stefnats/mediastorm-androidtv/commit/845471ca931dfcbfdb6475dad9306b14e262cdcc) | [codex/profile-taste-onboarding-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-taste-onboarding-controls) |
| [mediastorm-androidtv 13](https://github.com/godver3/mediastorm-androidtv/pull/13) | Deferred | [`086344580358`](https://github.com/stefnats/mediastorm-androidtv/commit/0863445803586e669e9e231f2c272fc470fab4fd) | [codex/profile-list-load-isolation](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-list-load-isolation) |
| [mediastorm-androidtv 14](https://github.com/godver3/mediastorm-androidtv/pull/14) | Deferred | [`ad131004009d`](https://github.com/stefnats/mediastorm-androidtv/commit/ad131004009d0bf6144739d0648b698f6bf95dce) | [codex/profile-personal-area-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-personal-area-controls) |
| [mediastorm-androidtv 15](https://github.com/godver3/mediastorm-androidtv/pull/15) | Deferred | [`c299806b05c9`](https://github.com/stefnats/mediastorm-androidtv/commit/c299806b05c9b9f378cba0215d297d4bea96a5c0) | [codex/profile-episode-autoplay-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-episode-autoplay-controls) |
| [mediastorm-androidtv 16](https://github.com/godver3/mediastorm-androidtv/pull/16) | Deferred | [`d119e56cd2cc`](https://github.com/stefnats/mediastorm-androidtv/commit/d119e56cd2cc17eaa618da1d5f0f10b4bcbeff53) | [codex/profile-post-play-controls](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-post-play-controls) |
| [mediastorm-androidtv 17](https://github.com/godver3/mediastorm-androidtv/pull/17) | Deferred | [`064bc5b48d20`](https://github.com/stefnats/mediastorm-androidtv/commit/064bc5b48d2053bfb966a30c8862f2239e2602eb) | [codex/profile-request-context](https://github.com/stefnats/mediastorm-androidtv/tree/codex/profile-request-context) |
| [mediastorm-androidtv 18](https://github.com/godver3/mediastorm-androidtv/pull/18) | Deferred | [`660a9de6bd3c`](https://github.com/stefnats/mediastorm-androidtv/commit/660a9de6bd3c65f08d9c96b296c3d1fb28036083) | [codex/live-station-groups](https://github.com/stefnats/mediastorm-androidtv/tree/codex/live-station-groups) |
