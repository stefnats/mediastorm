//go:build linux

package datastore

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestBridgeSelectedLimiterCandidatesDenialBeforePreparation(t *testing.T) {
	for _, kind := range []string{"nil", "empty", "over-three", "empty-id", "whitespace-id", "invalid-utf8", "duplicate", "foreign-account", "missing-candidate", "extra-candidate", "candidate-set-race"} {
		t.Run(kind, func(t *testing.T) {
			var preparations atomic.Int64
			trace := &bridgeSessionTransactionTrace{start: func(_ *pgx.Conn, sql string) {
				if strings.Contains(sql, "a.password_hash") {
					preparations.Add(1)
				}
			}}
			f := bridgeSelectedFixture(t, trace, nil)
			r, _ := bridgeWorkflowRequest(t)
			o := bridgeWorkflowObservation(t, f, r)
			expected := []BridgeLoginCandidate{{AccountID: "actor", ProfileID: "target"}}
			switch kind {
			case "nil":
				expected = nil
			case "empty":
				expected = []BridgeLoginCandidate{}
			case "over-three":
				expected = append(expected, expected[0], expected[0], expected[0])
			case "empty-id":
				expected[0].AccountID = ""
			case "whitespace-id":
				expected[0].ProfileID = " target "
			case "invalid-utf8":
				expected[0].ProfileID = string([]byte{255})
			case "duplicate":
				expected = append(expected, expected[0])
			case "foreign-account":
				expected[0].AccountID = "other"
			case "missing-candidate":
				bridgeSelectedAdd(t, f, "other-profile", "other", "synthetic wrong", false)
			case "extra-candidate":
				expected = append(expected, BridgeLoginCandidate{AccountID: "other", ProfileID: "missing"})
			case "candidate-set-race":
				var err error
				expected, err = f.issuer.ResolvePasswordCandidates(r, "Shared", "synthetic-device", o)
				if err != nil {
					t.Fatal(err)
				}
				bridgeSelectedAdd(t, f, "other-profile", "other", "synthetic wrong", false)
			}
			proof, err := f.issuer.VerifySelectedPasswordForCandidates(r, "Shared", "synthetic-device", parentTestPassword, o, expected)
			if proof != nil {
				proof.Close()
			}
			if !errors.Is(err, ErrBridgeCredentialChanged) || proof != nil || preparations.Load() != 0 || bridgeWorkflowCount(f) != 0 || f.count(t) != 0 {
				t.Fatal("limiter selectors reached credential preparation or mint", kind, err, preparations.Load())
			}
		})
	}
}

func TestBridgeSelectedLimiterCandidatesExactSetAndCopy(t *testing.T) {
	for _, kind := range []string{"exact", "reordered-all-candidates", "caller-slice-copy"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, data pgx.TraceQueryEndData) {
				if kind == "caller-slice-copy" && sql == bridgeLoginCandidateQuery && data.Err == nil {
					once.Do(func() { close(entered); <-proceed })
				}
			}}
			f := bridgeSelectedFixture(t, trace, nil)
			r, _ := bridgeWorkflowRequest(t)
			o := bridgeWorkflowObservation(t, f, r)
			expected := []BridgeLoginCandidate{{AccountID: "actor", ProfileID: "target"}}
			if kind == "reordered-all-candidates" {
				bridgeSelectedAdd(t, f, "other-profile", "other", "synthetic wrong", false)
				expected = append(expected, BridgeLoginCandidate{AccountID: "other", ProfileID: "other-profile"})
			}
			var proof *BridgeSelectedPasswordProof
			var err error
			if kind == "caller-slice-copy" {
				done := make(chan struct{})
				var unblock sync.Once
				t.Cleanup(func() {
					unblock.Do(func() { close(proceed) })
					bridgeSessionAwait(t, done)
					if proof != nil {
						proof.Close()
					}
				})
				go func() {
					proof, err = f.issuer.VerifySelectedPasswordForCandidates(r, "Shared", "synthetic-device", parentTestPassword, o, expected)
					close(done)
				}()
				bridgeSessionAwait(t, entered)
				expected[0] = BridgeLoginCandidate{AccountID: "foreign", ProfileID: "foreign"}
				unblock.Do(func() { close(proceed) })
				bridgeSessionAwait(t, done)
			} else {
				proof, err = f.issuer.VerifySelectedPasswordForCandidates(r, "Shared", "synthetic-device", parentTestPassword, o, expected)
			}
			if err != nil || proof == nil {
				t.Fatal("genuine constrained verification", err)
			}
			t.Cleanup(proof.Close)
			receipt := bridgeSelectedMint(t, f, r, proof, o)
			bridgeSelectedRelease(t, f, r, receipt)
		})
	}
}
