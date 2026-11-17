package profile_recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"novastream/config"
)

func TestParentKidsFlagClientRecoveryCopiesCheckedStoredRightsWithoutWrites(t *testing.T) {
	for _, management := range []bool{false, true} {
		for _, rights := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
			t.Run(fmt.Sprintf("management=%t/live=%t/recording=%t", management, rights[0], rights[1]), func(t *testing.T) {
				f := recoveryCoordinator(t)
				// The local projection predates this real independent database change.
				// These are saved facts, not a grant to interrupt live playback/recording.
				f.pg.exec(t, `UPDATE users SET name='Saved target',allow_live_takeover=$2,allow_recording_interruption=$3,plex_account_id='synthetic-private-account-value' WHERE id=$1`, f.target, rights[0], rights[1])
				f.pg.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES($1,'{"future":{"value":"synthetic-private-settings"},"display":{"simpleMode":false}}')`, f.target)
				request := f.request(t, t.Context())
				before := f.raw(t)
				var result *Result
				var err error
				if management {
					result, err = f.service.RecoverManagement(request, f.target)
				} else {
					result, err = f.service.RecoverAPI(request, f.target, func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} })
				}
				if err != nil || result == nil {
					t.Fatal("actual checked recovery failed", err)
				}
				if result.Profile.AllowLiveTakeover != rights[0] || result.Profile.AllowRecordingInterruption != rights[1] {
					t.Fatal("stored rights were guessed from old cache or omitted")
				}
				adopted, ok := f.users.Get(f.target)
				if !ok || adopted.AllowLiveTakeover != result.Profile.AllowLiveTakeover || adopted.AllowRecordingInterruption != result.Profile.AllowRecordingInterruption {
					t.Fatal("DTO differs from the actual adopted Users snapshot")
				}
				if f.raw(t) != before {
					t.Fatal("additional DTO facts introduced database writes")
				}
				encoded, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				var envelope map[string]json.RawMessage
				if json.Unmarshal(encoded, &envelope) != nil || len(envelope) != 2 {
					t.Fatal("unexpected response envelope")
				}
				var profile map[string]json.RawMessage
				if json.Unmarshal(envelope["profile"], &profile) != nil || len(profile) != 10 {
					t.Fatal("safe profile is not the precise ten-field projection")
				}
				allowed := []string{"id", "name", "isKidsProfile", "kidsMode", "kidsMaxRating", "kidsMaxMovieRating", "kidsMaxTVRating", "kidsAllowedLists", "allowLiveTakeover", "allowRecordingInterruption"}
				for _, key := range allowed {
					if _, ok := profile[key]; !ok {
						t.Fatal("missing safe profile field", key)
					}
				}
				for _, key := range []string{"allowLiveTakeover", "allowRecordingInterruption"} {
					var value bool
					if json.Unmarshal(profile[key], &value) != nil || string(profile[key]) == "null" {
						t.Fatal("stored permission flag is not a required boolean", key)
					}
				}
				if strings.Contains(string(encoded), "synthetic-private") {
					t.Fatal("private source/settings data escaped the safe DTO")
				}
			})
		}
	}
}

func TestParentKidsFlagClientRecoveryFieldsDoNotWidenOwnership(t *testing.T) {
	f := recoveryCoordinator(t)
	cached, _ := f.users.Get(f.target)
	f.pg.exec(t, `UPDATE users SET account_id='other',allow_live_takeover=true,allow_recording_interruption=true WHERE id=$1`, f.target)
	request := f.request(t, context.Background())
	before := f.raw(t)
	result, err := f.service.RecoverManagement(request, f.target)
	if result != nil || !errors.Is(err, ErrNotFound) {
		t.Fatal("safe rights became an ownership permission", err)
	}
	after, _ := f.users.Get(f.target)
	if !reflect.DeepEqual(cached, after) || f.raw(t) != before {
		t.Fatal("foreign recovery adopted facts or wrote state")
	}
}
