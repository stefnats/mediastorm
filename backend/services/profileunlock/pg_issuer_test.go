package profileunlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/users"
)

type issuerFacadeFixture struct {
	ds      *datastore.DataStore
	users   *users.Service
	session models.Session
}

func issuerFacadePG(t *testing.T) *issuerFacadeFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL PIN issuance fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket database required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect own fixture")
	}
	name := fmt.Sprintf("synthetic_pin_facade_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatal("create own role")
	}
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("create own schema")
	}
	query := u.Query()
	query.Set("search_path", pgx.Identifier{name}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+name)
	query.Set("application_name", name)
	u.RawQuery = query.Encode()
	ds, err := datastore.New(t.Context(), u.String())
	if err != nil {
		t.Fatalf("create own migrated store: %v", err)
	}
	t.Cleanup(func() {
		ds.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error("drop own schema")
		}
		if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic actor account password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor',$1),('other','Synthetic Other',$1)`, string(hash)); err != nil {
		t.Fatal("seed own accounts")
	}
	if _, err = ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic Target')`); err != nil {
		t.Fatal("seed own profile")
	}
	svc, err := users.NewServiceWithStore(ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.SetPin("target", "2580"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "SYNTHETIC_PIN_FACADE_SESSION", AccountID: "actor", ClientID: "synthetic-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err = ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	return &issuerFacadeFixture{ds: ds, users: svc, session: session}
}
func (f *issuerFacadeFixture) request(ctx context.Context) *http.Request {
	r := httptest.NewRequest("POST", "http://synthetic.invalid/internal-pin-issuance", nil).WithContext(ctx)
	ctx = context.WithValue(r.Context(), auth.ContextKeySession, f.session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, f.session.IsMaster)
	return r.WithContext(ctx)
}
func (f *issuerFacadeFixture) prepare(t *testing.T, issuer *PGPinIssuer) (*PGPinIntent, *PGPinProof) {
	t.Helper()
	intent, err := issuer.Prepare(f.request(t.Context()), "target", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	proof, err := issuer.Verify(intent, "2580")
	if err != nil {
		t.Fatal(err)
	}
	return intent, proof
}
func (f *issuerFacadeFixture) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_unlocks`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPGPinFacadeActualServicePINChangeAfterVerifiedProof(t *testing.T) {
	for _, mode := range []string{"same service SetPin", "second service SetPin", "ClearPin", "Reassign"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			issuer := NewPGPinIssuer(f.ds)
			intent, proof := f.prepare(t, issuer)
			actual := f.users
			if mode == "second service SetPin" {
				var err error
				actual, err = users.NewServiceWithStore(f.ds, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch mode {
			case "ClearPin":
				_, err = actual.ClearPin("target")
			case "Reassign":
				_, err = actual.Reassign("target", "other")
			default:
				_, err = actual.SetPin("target", "9753")
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "second service SetPin" && f.users.PinFingerprint("target") == actual.PinFingerprint("target") {
				t.Fatal("second-instance test did not leave old local fingerprint stale")
			}
			receipt, err := issuer.Commit(intent, proof)
			if receipt != nil || !errors.Is(err, datastore.ErrPGPinChanged) || f.count(t) != 0 {
				t.Fatalf("real service change after old bcrypt issued grant: %v", err)
			}
		})
	}
}
func TestPGPinFacadeDoesNotAdoptIntoLegacyCache(t *testing.T) {
	f := issuerFacadePG(t)
	legacy := NewService(f.ds.ProfileUnlocks())
	issuer := NewPGPinIssuer(f.ds)
	intent, proof := f.prepare(t, issuer)
	receipt, err := issuer.Commit(intent, proof)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	if f.count(t) != 1 || len(legacy.grants) != 0 {
		t.Fatal("private commit published legacy cache")
	}
	legacy.RevokeProfile("target")
	if f.count(t) != 0 || len(legacy.grants) != 0 {
		t.Fatal("historical receipt revived revoked legacy state")
	}
	// The receipt has no method which could release a token, grant or permit.
	for _, value := range []any{intent, proof, receipt} {
		typ := reflect.TypeOf(value)
		for n := 0; n < typ.NumMethod(); n++ {
			name := typ.Method(n).Name
			if name != "Close" && name != "Format" && name != "MarshalJSON" {
				t.Fatalf("unexpected receipt accessor: %s", name)
			}
		}
		data, err := json.Marshal(value)
		if err != nil || string(data) != "null" {
			t.Fatal("opaque state exposed JSON")
		}
	}
}
func TestPGPinFacadeWrongIdentityHasNoPGAcquisition(t *testing.T) {
	for _, mode := range []string{"facade copy", "other issuer", "intent copy", "proof copy", "zero proof", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			issuer := NewPGPinIssuer(f.ds)
			intent, proof := f.prepare(t, issuer)
			switch mode {
			case "facade copy":
				copy := *issuer
				issuer = &copy
			case "other issuer":
				issuer = NewPGPinIssuer(f.ds)
			case "intent copy":
				copy := *intent
				intent = &copy
			case "proof copy":
				copy := *proof
				proof = &copy
			case "zero proof":
				proof = &PGPinProof{}
			case "replay":
				receipt, err := issuer.Commit(intent, proof)
				if err != nil {
					t.Fatal(err)
				}
				defer receipt.Close()
			}
			before := f.ds.Pool().Stat().AcquireCount()
			receipt, err := issuer.Commit(intent, proof)
			if receipt != nil || !errors.Is(err, datastore.ErrPGPinUsed) || f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatalf("invalid facade identity acquired PG: %v", err)
			}
		})
	}
}
