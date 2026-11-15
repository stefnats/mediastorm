# Review queue

PRs [81](https://github.com/godver3/mediastorm/pull/81), [82](https://github.com/godver3/mediastorm/pull/82) and [96](https://github.com/godver3/mediastorm/pull/96) were merged by the maintainer on 15 November 2026 at 19:30 Europe/Berlin. No own upstream PR is open in either repository. Deferred features remain in the forks pending scope discussion.

The table retains the reviewed source heads, pinned diffs and recorded validation for the three completed independent corrections.

| PR | Problem and result | Complete diff | Recorded validation |
| --- | --- | --- | --- |
| [82: asynchronous tests](https://github.com/godver3/mediastorm/pull/82) | Wait for cleanup and accept cancellation of a losing preflight request | [3 test files, +30/-4](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...f24fa1b9c82d23544401a160752f7a9dc81ac210) | 3 affected packages, 3,282 passing cases and 7 skips |
| [81: watch-room guest authority](https://github.com/godver3/mediastorm/pull/81) | Ending a room or removing a guest prevents fresh playback through its media grant | [8 files, +298/-8](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...72c4e347da9a49a75ff5fd6ec8eda88a09277712) | 56 service cases and 1 isolated grant-endpoint regression |
| [96: profile-owned preparation](https://github.com/godver3/mediastorm/pull/96) | A different profile receives its own preparation instead of an inaccessible warm reference | [2 files, +173/-9](https://github.com/stefnats/mediastorm/compare/8b278f9e00ed8c100839e3abf3b9b0f688c2dfe0...2f772452ab0bad47afda45fb41f6887d8bb1a887) | 195 service cases and 7 handler cases, including subtests |

The diffs above are pinned to the exact reviewed source heads. The tests were run earlier on those unchanged heads; this documentation update does not claim a new test run or GitHub CI. [Validation and review entry points](active-fixes.md) identify the relevant files and limitations.

## Feature proposals on hold

The earlier feature PRs are closed and their branches remain in the forks. [Deferred proposals](deferred-proposals.md) groups them by user outcome and describes candidate review boundaries. Those groups are planning topics, not combined merge requests. Further feature submissions wait for the maintainer's requested scope discussion.

The [maintainer's review concern](https://github.com/godver3/mediastorm/pull/97#issuecomment-6886025893) was the volume of submissions and their stacked changes. New proposals should each contain one agreed change against the then-current upstream, with explicit dependencies and fresh tests. A large integration branch is retained as an archive, not offered as the replacement review unit.

[Original PRs and preserved branches](source-archive.md) records all 39 original heads. It also links the historical combined tests; those results do not establish isolated acceptance of a future extracted feature.
