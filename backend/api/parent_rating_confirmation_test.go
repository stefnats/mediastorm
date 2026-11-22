package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var ratingAPIFields = map[string]string{"generic": "kids_max_rating", "movie": "kids_max_movie_rating", "tv": "kids_max_tv_rating"}
var ratingAPIPrefixes = map[string]string{"generic": "prg_", "movie": "prm_", "tv": "prt_", "pair": "prp_"}
var ratingAPIInitial = map[string]string{"generic": "base-generic", "movie": "base-movie", "tv": "base-tv"}

func ratingAPIFixture(t *testing.T) *publicPGPinFixture {
	t.Helper()
	f := parentListAPIFixture(t)
	if _, err := f.users.SetKidsMaxRating(f.open, ratingAPIInitial["generic"]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.users.SetKidsMaxMovieRating(f.open, ratingAPIInitial["movie"]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.users.SetKidsMaxTVRating(f.open, ratingAPIInitial["tv"]); err != nil {
		t.Fatal(err)
	}
	return f
}

func ratingAPIPath(profile, kind string) string {
	base := "/api/users/" + profile + "/kids/rating"
	if kind == "pair" {
		return base + "s"
	}
	if kind != "generic" {
		return base + "/" + kind
	}
	return base
}

func ratingAPIRequest(profile, kind, token, body, intent, password string) *http.Request {
	return parentListWritesRequest("PUT", ratingAPIPath(profile, kind), token, body, intent, password)
}

func ratingAPIBody(kind string) string {
	if kind == "pair" {
		return `{"movieRating":"new-movie","tvRating":"new-tv"}`
	}
	return `{"rating":"new-limit"}`
}

func ratingAPIChallenge(t *testing.T, f *publicPGPinFixture, kind, token, body string) string {
	t.Helper()
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	w := parentModeAPIServe(f, ratingAPIRequest(f.open, kind, token, body, "", ""))
	var value struct {
		Code         string `json:"code"`
		ParentIntent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, ratingAPIPrefixes[kind]) || len(value.ParentIntent) != 68 {
		t.Fatalf("registered %s rating route did not issue its original challenge: HTTP %d", kind, w.Code)
	}
	decoded, err := hex.DecodeString(value.ParentIntent[4:])
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value.ParentIntent[4:] {
		t.Fatal("rating intent is not canonical lowercase hex")
	}
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
		t.Fatal("unconfirmed rating request changed the target or its revision")
	}
	return value.ParentIntent
}

func TestParentRatingsRegisteredExactFieldsAndAtomicPair(t *testing.T) {
	type item struct {
		name, kind, body string
		wanted           map[string]string
		master           bool
	}
	var cases []item
	for _, kind := range []string{"generic", "movie", "tv"} {
		cases = append(cases,
			item{kind + " trim without normalization", kind, `{"rating":"  lower-custom  "}`, map[string]string{kind: "lower-custom"}, true},
			item{kind + " identical still writes", kind, parentListWritesJSON(t, map[string]string{"rating": ratingAPIInitial[kind]}), map[string]string{kind: ratingAPIInitial[kind]}, false},
			item{kind + " trailing and duplicate null", kind, `{"rating":"kept-string","RATING":null} {"ignored":true}`, map[string]string{kind: "kept-string"}, false},
			item{kind + " root null clears", kind, `null`, map[string]string{kind: ""}, false},
		)
	}
	exactScalar := `{"rating":"edge"}`
	exactScalar += strings.Repeat(" ", (16<<10)-len(exactScalar))
	exactPair := ratingAPIBody("pair")
	exactPair += strings.Repeat(" ", (16<<10)-len(exactPair))
	cases = append(cases,
		item{"generic exact body limit", "generic", exactScalar, map[string]string{"generic": "edge"}, false},
		item{"pair exact body limit", "pair", exactPair, map[string]string{"movie": "new-movie", "tv": "new-tv"}, false},
		item{"pair trims both without changing generic fallback", "pair", `{"movieRating":" \tcustom-movie ","tvRating":" custom-tv\n "}`, map[string]string{"movie": "custom-movie", "tv": "custom-tv"}, false},
		item{"master pair explicit empty preserves generic fallback", "pair", `{"movieRating":"","tvRating":""}`, map[string]string{"movie": "", "tv": ""}, true},
		item{"pair identical still writes once", "pair", `{"movieRating":"base-movie","tvRating":"base-tv"}`, map[string]string{"movie": "base-movie", "tv": "base-tv"}, false},
		item{"pair null then string and trailing value", "pair", `{"movieRating":null,"MOVIERATING":"new-movie","tvRating":"new-tv"} {"ignored":true}`, map[string]string{"movie": "new-movie", "tv": "new-tv"}, false},
	)
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			f := ratingAPIFixture(t)
			if item.master {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(false)
			before, other := parentModeAPIRow(t, f, f.open), parentModeAPIRow(t, f, f.foreign)
			version := parentListWritesVersion(t, f)
			intent := ratingAPIChallenge(t, f, item.kind, token, item.body)
			w := parentModeAPIServe(f, ratingAPIRequest(f.open, item.kind, token, item.body, intent, "synthetic-api-password"))
			if w.Code != 200 {
				t.Fatalf("confirmed %s rating operation failed: HTTP %d", item.kind, w.Code)
			}
			after := parentModeAPIRow(t, f, f.open)
			wantedFields := []string{"updated_at"}
			for kind, wanted := range item.wanted {
				field := ratingAPIFields[kind]
				var old, saved string
				if json.Unmarshal(before[field], &old) != nil || json.Unmarshal(after[field], &saved) != nil || saved != wanted {
					t.Fatal("rating operation changed the selected value or field identity")
				}
				if old != wanted {
					wantedFields = append(wantedFields, field)
				}
			}
			var changed []string
			for field, value := range before {
				if !bytes.Equal(value, after[field]) {
					changed = append(changed, field)
				}
			}
			sort.Strings(changed)
			wantedFields = parentExpectedFieldsWithShareRotation(t, before, after, wantedFields)
			if !reflect.DeepEqual(changed, wantedFields) || !reflect.DeepEqual(other, parentModeAPIRow(t, f, f.foreign)) || version == parentListWritesVersion(t, f) {
				t.Fatal("rating write changed an unrelated field/profile or skipped its established write")
			}
			profile, ok := f.users.Get(f.open)
			if !ok {
				t.Fatal("known rating result lost its profile")
			}
			actual := map[string]string{"generic": profile.KidsMaxRating, "movie": profile.KidsMaxMovieRating, "tv": profile.KidsMaxTVRating}
			for kind, original := range ratingAPIInitial {
				wanted, supplied := item.wanted[kind]
				if !supplied {
					wanted = original
				}
				if actual[kind] != wanted {
					t.Fatal("targeted users acknowledgement changed another rating or kept stale data")
				}
			}
			replay := parentModeAPIServe(f, ratingAPIRequest(f.open, item.kind, token, item.body, intent, "synthetic-api-password"))
			if replay.Code != 409 || !reflect.DeepEqual(after, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("consumed rating operation was replayed")
			}
		})
	}
}

func TestParentRatingsRegisteredOriginalRawBodyAndContext(t *testing.T) {
	for _, kind := range []string{"generic", "movie", "tv", "pair"} {
		for _, mutation := range []string{"whitespace bytes", "ignored trailing bytes"} {
			t.Run(kind+"/"+mutation, func(t *testing.T) {
				f := ratingAPIFixture(t)
				token, body := f.session(false), ratingAPIBody(kind)
				intent := ratingAPIChallenge(t, f, kind, token, body)
				if mutation == "whitespace bytes" {
					body += " "
				} else {
					body += ` {"ignored":true}`
				}
				before := parentModeAPIRow(t, f, f.open)
				w := parentModeAPIServe(f, ratingAPIRequest(f.open, kind, token, body, intent, "synthetic-api-password"))
				if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
					t.Fatal("semantically equivalent changed rating bytes adopted the original intent")
				}
			})
		}
	}
	for _, mutation := range []string{"session", "stored client", "actor role", "actual refresh", "concurrent generic fallback", "query", "duplicate header"} {
		t.Run("pair/"+mutation, func(t *testing.T) {
			f := ratingAPIFixture(t)
			token, body := f.session(false), ratingAPIBody("pair")
			intent := ratingAPIChallenge(t, f, "pair", token, body)
			switch mutation {
			case "session":
				token = f.session(false)
			case "stored client":
				f.exec(`UPDATE sessions SET client_id='synthetic-replacement-client' WHERE token=$1`, token)
			case "actor role":
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			case "actual refresh":
				response := f.do("POST", "/api/auth/refresh", token, "", nil)
				var value struct {
					Token string `json:"token"`
				}
				if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &value) != nil || value.Token == "" || value.Token == token {
					t.Fatal("registered refresh did not replace the original session")
				}
				token = value.Token
			case "concurrent generic fallback":
				if _, err := f.users.SetKidsMaxRating(f.open, "newer-fallback"); err != nil {
					t.Fatal(err)
				}
			}
			r := ratingAPIRequest(f.open, "pair", token, body, intent, "synthetic-api-password")
			if mutation == "query" {
				r.URL.RawQuery = "changed=1"
			}
			if mutation == "duplicate header" {
				r.Header.Add("X-Parent-Intent", intent)
			}
			before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
			w := parentModeAPIServe(f, r)
			if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
				t.Fatalf("pair adopted changed %s: HTTP %d", mutation, w.Code)
			}
		})
	}
}

func TestParentRatingsRegisteredActionIsolation(t *testing.T) {
	f := ratingAPIFixture(t)
	token := f.session(false)
	intents := map[string]string{}
	for _, kind := range []string{"generic", "movie", "tv", "pair"} {
		intents[kind] = ratingAPIChallenge(t, f, kind, token, ratingAPIBody(kind))
	}
	mode := parentModeAPIChallenge(t, f, token, "content_list")
	removal := parentListAPIChallenge(t, f, parentListAPIPath(f.open, parentListAPIURL), token)
	addBody, replaceBody := `{"url":"new-list"}`, `{"lists":["new-list"]}`
	add := parentListWritesChallenge(t, f, "POST", token, addBody)
	replace := parentListWritesChallenge(t, f, "PUT", token, replaceBody)
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	var requests []*http.Request
	for source, intent := range intents {
		for _, target := range []string{"generic", "movie", "tv", "pair"} {
			if source != target {
				requests = append(requests, ratingAPIRequest(f.open, target, token, ratingAPIBody(target), intent, "synthetic-api-password"))
			}
		}
	}
	for _, other := range []string{mode, removal, add, replace} {
		requests = append(requests, ratingAPIRequest(f.open, "pair", token, ratingAPIBody("pair"), other, "synthetic-api-password"))
	}
	requests = append(requests,
		parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", intents["generic"], "synthetic-api-password"),
		parentListAPIRequest(parentListAPIPath(f.open, parentListAPIURL), token, intents["movie"], "synthetic-api-password"),
		parentListWritesRequest("POST", parentListWritesPath(f.open), token, addBody, intents["tv"], "synthetic-api-password"),
		parentListWritesRequest("PUT", parentListWritesPath(f.open), token, replaceBody, intents["pair"], "synthetic-api-password"),
	)
	for _, request := range requests {
		w := parentModeAPIServe(f, request)
		if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
			t.Fatal("an intent was accepted by another parent rating/list/mode action")
		}
	}
}

func TestParentRatingsRegisteredInvalidPairAndWrongPassword(t *testing.T) {
	f := ratingAPIFixture(t)
	token := f.session(false)
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	for _, body := range []string{`null`, `{}`, `{"movieRating":"x"}`, `{"movieRating":null,"tvRating":"x"}`, `{"movieRating":"x","tvRating":42}`, `{"movieRating":"x","MOVIERATING":null,"tvRating":"x"}`, `{"movieRating":"x","tvRating":"x","unknown":true}`} {
		w := parentModeAPIServe(f, ratingAPIRequest(f.open, "pair", token, body, "", ""))
		if w.Code != 400 || strings.Contains(w.Body.String(), "parentIntent") || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatal("invalid pair obtained confirmation or changed one field")
		}
	}
	for _, kind := range []string{"generic", "movie", "tv", "pair"} {
		body := ratingAPIBody(kind) + strings.Repeat(" ", 16<<10)
		w := parentModeAPIServe(f, ratingAPIRequest(f.open, kind, token, body, "", ""))
		if w.Code != 413 || strings.Contains(w.Body.String(), "parentIntent") || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatal("oversized rating body was accepted or changed the target")
		}
	}
	body := ratingAPIBody("pair")
	intent := ratingAPIChallenge(t, f, "pair", token, body)
	w := parentModeAPIServe(f, ratingAPIRequest(f.open, "pair", token, body, intent, "synthetic-wrong-password"))
	if w.Code != 403 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("wrong password changed either rating field")
	}
	w = parentModeAPIServe(f, ratingAPIRequest(f.open, "pair", token, body, intent, "synthetic-api-password"))
	if w.Code != 409 {
		t.Fatal("failed password proof could reuse the consumed pair intent")
	}
	intent = ratingAPIChallenge(t, f, "pair", token, body)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	parentModeAPIServe(f, ratingAPIRequest(f.open, "pair", token, body, intent, "synthetic-api-password").WithContext(ctx))
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
		t.Fatal("an already canceled pair request changed its target")
	}
}
