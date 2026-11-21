package datastore

// Historical compatibility is DATA correlation, not provenance authentication
// or authorization. Imported DATA may be unverifiable. A caller must already
// own the genuine current original transaction, authority and the 085 row lock.
func compatibleRecordingHistory(old, current *recordingProgressHistoryData) bool {
	if old == nil || current == nil || len(old.tuple.spans) < 2 || len(current.tuple.spans) < 2 {
		return false
	}
	a, b := old.envelope, current.envelope
	x, y := old.tuple, current.tuple
	// The legacy missingStrongEvidence flag and an output digest never replace
	// actual input/tool/recipe evidence. ELF hashes do not certify shared libraries.
	if a.Tool == nil || a.Transform == nil || b.Tool == nil || b.Transform == nil || *a.Tool != *b.Tool || *a.Transform != *b.Transform || a.Ledger != b.Ledger || x.recording != y.recording || x.duration != y.duration || x.status != y.status || x.incomplete != y.incomplete || x.gaps != y.gaps || len(x.spans) != len(y.spans) || len(a.Manifest) != len(b.Manifest) || len(a.Manifest) != len(x.spans) || len(b.Manifest) != len(y.spans) {
		return false
	}
	for i, span := range x.spans {
		if span != y.spans[i] || a.Manifest[i] != b.Manifest[i] {
			return false
		}
	}
	// Artifact/incarnation/map/observation times change on a fresh preparation.
	// Matroska bytes/length may differ. Only exactly equal measured coordinates
	// support copying this bounded position; there is no guessed anchor/tolerance.
	return a.Position >= 0 && a.Position <= y.duration && a.End == (a.Position == y.duration)
}

// A fresh restored incarnation has no current writer yet. Its first explicit
// claim can use verified historical DATA; a deliberately retired nonzero writer
// cannot be resurrected through this action.
func historicalRecordingStateEligible(state *recordingPlaybackState) bool {
	return state != nil && (!state.tombstone || state.revision == 0 && state.generation == 0 && state.operationID == nil)
}
func recordingHistoricalResumePosition(guard *settingsProgressTx, s *recordingProgressIntentState, state *recordingPlaybackState) (int64, string, error) {
	if s == nil || s.history == nil || !historicalRecordingStateEligible(state) {
		return 0, "unverifiable", nil
	}
	historical, err := guard.readRecordingProgressHistory(s.identity.recordingID)
	if err != nil {
		return 0, "", ErrRecordingProgressUnavailable
	}
	if historical.state == "absent" {
		return 0, "absent", nil
	}
	if historical.state != "historical-data" || !compatibleRecordingHistory(historical.data, s.history) {
		return 0, "unverifiable", nil
	}
	return historical.data.envelope.Position, "compatible", nil
}
