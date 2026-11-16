package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/models"
)

// Real public handlers encode these synthetic fixtures; no recorder, stream,
// provider or filesystem media is opened by this wire-only service.
type captureWireService struct {
	fakeRecordingService
	items          []models.Recording
	created        models.Recording
	createdProfile string
}

func (s *captureWireService) List(f models.RecordingListFilter) ([]models.Recording, error) {
	out := []models.Recording{}
	for _, r := range s.items {
		if f.IncludeAll || f.UserID == r.UserID {
			out = append(out, r)
		}
	}
	return out, nil
}
func (s *captureWireService) Get(id string) (*models.Recording, error) {
	for _, r := range s.items {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, fmt.Errorf("synthetic recording not found")
}
func (s *captureWireService) CreateFromEPG(r models.CreateEPGRecordingRequest) (models.Recording, error) {
	s.createdProfile = r.ProfileID
	return s.created, nil
}
func (s *captureWireService) CreateTimeBlock(r models.CreateTimeBlockRecordingRequest) (models.Recording, error) {
	s.createdProfile = r.ProfileID
	return s.created, nil
}

func captureWireRecording(id string, at time.Time) models.Recording {
	ended := at.Add(2 * time.Minute)
	return models.Recording{ID: id, UserID: "adult", Type: models.RecordingTypeEPG, Status: models.RecordingStatusFailed,
		ChannelID: "synthetic-channel", ChannelName: "Synthetic station", Title: "Synthetic recording",
		SourceURL: "https://example.invalid/SYNTHETIC_SOURCE_TOKEN", OutputPath: "/synthetic-private/" + id + "/part-1.ts", OutputSizeBytes: 752,
		StartAt: at.Add(-time.Minute), EndAt: at.Add(10 * time.Minute), ActualStartAt: &at, ActualEndAt: &ended,
		Error:     "SYNTHETIC_DIAGNOSTIC_TOKEN https://example.invalid/SYNTHETIC_SOURCE_TOKEN /synthetic-private/capture.ts",
		CreatedAt: at.Add(-time.Hour), UpdatedAt: ended,
		Capture: &models.RecordingCapture{Version: 1, Phase: "interrupted", Incomplete: true, SourceChanges: 1, StationID: "PRIVATE_STATION_CONTEXT", RunID: "PRIVATE_RUN_CONTEXT",
			Parts: []models.RecordingPart{{Index: 1, SourceID: "source-one", ChannelID: "channel-one", StartedAt: at, EndedAt: &ended, Bytes: 752, EndReason: "source_ended", StreamPath: "recording:" + id + "/parts/1"},
				{Index: 2, SourceID: "source-two", ChannelID: "channel-two", StartedAt: ended, EndedAt: &ended, Bytes: 1128, EndReason: "capture_failed", StreamPath: "recording:" + id + "/parts/2"}},
			Gaps: []models.RecordingGap{{StartedAt: ended, EndedAt: &ended, Reason: "source_retry"}}}}
}

func captureWireRequest(method, target, account string, body []byte) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req.WithContext(context.WithValue(req.Context(), auth.ContextKeyAccountID, account))
}

type captureWireResponse struct {
	kind string
	data []byte
}

func recordingCaptureWireResponses(t *testing.T) map[string]captureWireResponse {
	t.Helper()
	at := time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)
	multi := captureWireRecording("multipart", at)
	waiting := captureWireRecording("waiting", at)
	waiting.Status = models.RecordingStatusRunning
	waiting.ActualEndAt = nil
	waiting.Capture.Phase = "waiting_capacity"
	waiting.Capture.SourceChanges = 0
	waiting.Capture.Parts = waiting.Capture.Parts[:1]
	waiting.Capture.Gaps = []models.RecordingGap{{StartedAt: at.Add(2 * time.Minute), Reason: "capacity"}}
	active := captureWireRecording("capturing", at)
	active.Status = models.RecordingStatusRunning
	active.Error = ""
	active.ActualEndAt = nil
	active.Capture.Phase = "capturing"
	active.Capture.Parts[1].EndedAt = nil
	active.Capture.Parts[1].EndReason = ""
	active.Capture.Parts[1].Bytes = 188
	legacy := captureWireRecording("legacy", at)
	legacy.Status = models.RecordingStatusCompleted
	legacy.OutputPath = `C:\synthetic-private\legacy\part-1.ts`
	legacy.Capture = nil
	legacy.Error = ""
	pending := captureWireRecording("pending", at)
	pending.Status = models.RecordingStatusPending
	pending.Capture = nil
	pending.OutputSizeBytes = 0
	pending.ActualStartAt = nil
	pending.ActualEndAt = nil
	restored := captureWireRecording("restored", at)
	restored.Status = models.RecordingStatusRunning
	restored.ActualEndAt = nil
	restored.Capture.Parts[1].EndedAt = nil
	restored.Capture.Parts[1].EndReason = ""
	models.RestoreRecordingCapture(&restored)
	restored.Capture.Parts[0].StreamPath = "recording:restored/parts/1" // Imported claims never restore serving authority.
	emptyFirst := captureWireRecording("first-empty", at)
	emptyFirst.OutputSizeBytes = 0
	emptyFirst.Capture.Parts[0].Bytes = 0
	emptyFirst.Capture.Parts[0].StreamPath = ""
	emptyFirst.Capture.Parts[1].Bytes = 752
	svc := &captureWireService{items: []models.Recording{multi, waiting, active, legacy, pending, restored, emptyFirst}, created: pending}
	users := &fakeRecordingUsersProvider{users: []models.User{{ID: "adult", AccountID: "account-one"}, {ID: "foreign", AccountID: "account-two"}}}
	handler := NewRecordingsHandler(svc, users)
	before, err := json.Marshal(svc.items)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]captureWireResponse{}
	names := []string{"multipart-failed.json", "waiting-capacity.json", "capturing-parts.json", "legacy-completed.json", "pending.json", "restored.json", "first-empty.json"}
	assertPublic := func(data []byte) {
		t.Helper()
		for _, private := range []string{"SYNTHETIC_SOURCE_TOKEN", "SYNTHETIC_DIAGNOSTIC_TOKEN", "synthetic-private", "PRIVATE_STATION_CONTEXT", "PRIVATE_RUN_CONTEXT", `"restored":true`} {
			if bytes.Contains(data, []byte(private)) {
				t.Fatalf("public handler leaked synthetic private marker %q", private)
			}
		}
	}
	for i, r := range svc.items {
		req := mux.SetURLVars(captureWireRequest(http.MethodGet, "/api/live/recordings/"+r.ID+"?profileId=adult", "account-one", nil), map[string]string{"recordingID": r.ID})
		reply := httptest.NewRecorder()
		handler.Get(reply, req)
		if reply.Code != http.StatusOK {
			t.Fatalf("Get status=%d", reply.Code)
		}
		assertPublic(reply.Body.Bytes())
		var decoded models.Recording
		if err = json.Unmarshal(reply.Body.Bytes(), &decoded); err != nil {
			t.Fatal(err)
		}
		wantPath := "part-1.ts"
		if r.Capture != nil && r.Capture.Restored {
			wantPath = ""
		}
		if decoded.ID != r.ID || decoded.SourceURL != "" || decoded.OutputPath != wantPath {
			t.Fatalf("public recording %s lost identity or retained private location", r.ID)
		}
		if r.Capture == nil && decoded.Capture != nil {
			t.Fatal("legacy/pending response invented capture state")
		}
		if decoded.Capture != nil && (decoded.Capture.Parts == nil || decoded.Capture.Gaps == nil) {
			t.Fatal("capture arrays must encode empty arrays rather than null")
		}
		if r.ID == "restored" {
			for _, p := range decoded.Capture.Parts {
				if p.StreamPath != "" {
					t.Fatal("restored bytes became an authorised source path")
				}
			}
		}
		if r.ID == "capturing" && (decoded.Capture.Parts[1].EndedAt != nil || decoded.Capture.Parts[1].EndReason != "" || decoded.Capture.Parts[1].Bytes != 188) {
			t.Fatal("active snapshot was converted to a closed or final part")
		}
		out[names[i]] = captureWireResponse{"RecordingPublic", bytes.Clone(reply.Body.Bytes())}
		denied := httptest.NewRecorder()
		handler.Get(denied, mux.SetURLVars(captureWireRequest(http.MethodGet, "/api/live/recordings/"+r.ID, "account-two", nil), map[string]string{"recordingID": r.ID}))
		if denied.Code != http.StatusNotFound {
			t.Fatalf("foreign recording Get status=%d", denied.Code)
		}
	}
	list := httptest.NewRecorder()
	handler.List(list, captureWireRequest(http.MethodGet, "/api/live/recordings?profileId=adult", "account-one", nil))
	if list.Code != http.StatusOK {
		t.Fatal("owned list failed")
	}
	assertPublic(list.Body.Bytes())
	var listed struct {
		Recordings []models.Recording `json:"recordings"`
	}
	if err = json.Unmarshal(list.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Recordings) != len(svc.items) {
		t.Fatal("list omitted synthetic recordings")
	}
	out["recordings-list.json"] = captureWireResponse{"RecordingList", bytes.Clone(list.Body.Bytes())}
	denied := httptest.NewRecorder()
	handler.List(denied, captureWireRequest(http.MethodGet, "/api/live/recordings?profileId=foreign", "account-one", nil))
	if denied.Code != http.StatusNotFound {
		t.Fatal("foreign list was accepted")
	}
	createBody, err := json.Marshal(map[string]any{"profileId": "adult", "channelId": "synthetic-channel", "channelName": "Synthetic station", "title": "Synthetic recording", "sourceUrl": "https://example.invalid/never-open", "start": at.Format(time.RFC3339), "stop": at.Add(time.Minute).Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		call       http.HandlerFunc
		kind       models.RecordingType
	}{{"create-epg.json", "/api/live/recordings/epg", handler.CreateEPG, models.RecordingTypeEPG}, {"create-block.json", "/api/live/recordings/time-block", handler.CreateTimeBlock, models.RecordingTypeTimeBlock}} {
		svc.created.Type = tc.kind
		reply := httptest.NewRecorder()
		tc.call(reply, captureWireRequest(http.MethodPost, tc.path, "account-one", createBody))
		if reply.Code != http.StatusCreated || svc.createdProfile != "adult" {
			t.Fatalf("create status=%d; profile=%q", reply.Code, svc.createdProfile)
		}
		assertPublic(reply.Body.Bytes())
		out[tc.name] = captureWireResponse{"RecordingPublic", bytes.Clone(reply.Body.Bytes())}
		denied := httptest.NewRecorder()
		tc.call(denied, captureWireRequest(http.MethodPost, tc.path, "account-two", createBody))
		if denied.Code != http.StatusNotFound {
			t.Fatal("foreign create was accepted")
		}
	}
	after, err := json.Marshal(svc.items)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("public projection mutated private repository capture/source state")
	}
	if svc.created.SourceURL == "" || !strings.HasPrefix(svc.created.OutputPath, "/synthetic-private/") {
		t.Fatal("create projection mutated stored private fields")
	}
	return out
}

func TestRecordingCaptureWireResponses(t *testing.T) {
	responses := recordingCaptureWireResponses(t)
	root := os.Getenv("DVR_CAPTURE_CONTRACT_DIR")
	if root == "" {
		return
	} // Export is optional; all handler assertions above always run.
	if !filepath.IsAbs(root) {
		t.Fatal("fixture export root must be absolute")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	hashes := map[string]string{}
	names := make([]string, 0, len(responses))
	for name := range responses {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		response := responses[name]
		if err := os.WriteFile(filepath.Join(root, name), response.data, 0644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(response.data)
		hashes[name] = hex.EncodeToString(sum[:])
		files[name] = response.kind
	}
	for name, value := range map[string]any{"manifest.json": map[string]any{"version": 1, "files": files, "origin": "actual RecordingsHandler.Get/List/CreateEPG/CreateTimeBlock with owned synthetic account/profile"}, "sha256.json": hashes} {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecordingCaptureWireOpenAPI(t *testing.T) {
	responses := recordingCaptureWireResponses(t)
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	schema := doc.Components.Schemas["RecordingPublic"]
	if schema == nil || schema.Value == nil {
		t.Fatal("RecordingPublic component is required")
	}
	for name, response := range responses {
		t.Run(name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal(response.data, &body); err != nil {
				t.Fatal(err)
			}
			if response.kind == "RecordingList" {
				items, ok := body["recordings"].([]any)
				if !ok {
					t.Fatal("missing recordings array")
				}
				for _, item := range items {
					if err := schema.Value.VisitJSON(item); err != nil {
						t.Fatal(err)
					}
				}
			} else if err := schema.Value.VisitJSON(body); err != nil {
				t.Fatal(err)
			}
		})
	}
}
