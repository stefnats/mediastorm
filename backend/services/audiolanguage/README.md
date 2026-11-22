# Optional spoken audio language checks

The release controls can compare two short samples of the currently selected
container audio track with the effective requested language. The default is off.
The server owner can configure `playback.audioLanguageCheckUrl` with the complete
`/inference` URL of an existing local whisper.cpp server and multilingual model.
This feature neither downloads a model nor starts or installs a service.

The endpoint must support current whisper.cpp `verbose_json`, including
`language_probabilities`, segment `avg_logprob`, `no_speech_prob` and timestamps.
See the [upstream server implementation](https://github.com/ggml-org/whisper.cpp/blob/master/examples/server/server.cpp).
Only the selected audio samples are sent, under a generic filename. Source URLs,
release names, profile identifiers and transcripts are not retained in the result.

The viewer explicitly starts a check. Playback continues. Two non-overlapping
20-second, mono 16-kHz PCM windows must agree with sufficient speech evidence and
language probability. Silent, incomplete, mixed-language and uncertain samples,
older incompatible endpoints and unsupported non-seekable sources remain unknown.
Checks do not queue behind another active check; request cancellation and a
90-second total budget bound their work.

Results are `likely_match`, `likely_mismatch` or `unknown`. A sample-based hint
does not establish the language of the complete soundtrack, identify regional
dubs, diagnose lip sync or prove a defective file. No report, block, search,
replacement or automatic handoff follows from a result. Existing source controls
remain available for a deliberate report or alternative selection.

The API checks the current source version, concrete container track and effective
audio request before sampling and before publishing a result. Native and Web
consumers discard results after profile, source or track changes. Actual model
accuracy and device playback require separate acceptance; synthetic protocol tests
are not model-accuracy tests.
