//go:build linux

package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/internal/importer"
	"path/filepath"
	"testing"
)

func TestSABPublicationBootstrap(t *testing.T) {
	for _, name := range []string{"original-finished-startup-only", "foreign-store-and-closed-startup-denied"} {
		t.Run(name, func(t *testing.T) {
			f := bootstrapPG(t)
			settings := config.DefaultSettings()
			settings.Database.URL = f.raw
			if e := f.manager.Save(settings); e != nil {
				t.Fatal(e)
			}
			startup, e := Begin(t.Context(), f.manager, "")
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = startup.Close() })
			db, e := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "queue.db")})
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { db.Close() })
			source, e := importer.NewService(importer.ServiceConfig{}, nil, db, nil, nil)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = source.Close() })
			if value, e := startup.NewSABLibraryPublicationIssuer(nil, source); e == nil || value != nil {
				t.Fatal("factory silently started Finish")
			}
			store, e := startup.Migrate(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			if e = startup.Finish(); e != nil {
				t.Fatal(e)
			}
			issuer, e := startup.NewSABLibraryPublicationIssuer(store, source)
			if e != nil || issuer == nil {
				t.Fatal("actual original finished startup", e)
			}
			defer issuer.Close()
			switch name {
			case "original-finished-startup-only":
				copied := *startup
				if value, e := copied.NewSABLibraryPublicationIssuer(store, source); e == nil || value != nil {
					t.Fatal("copied Startup manufactured publication issuer")
				}
				if value, e := startup.NewSABLibraryPublicationIssuer(store, nil); e == nil || value != nil {
					t.Fatal("missing original importer accepted")
				}
			case "foreign-store-and-closed-startup-denied":
				foreign, e := datastore.New(t.Context(), f.raw)
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				if value, e := startup.NewSABLibraryPublicationIssuer(foreign, source); e == nil || value != nil {
					t.Fatal("different actual SQL pool borrowed original coordinator")
				}
				issuer.Close()
				if e = startup.Close(); e != nil {
					t.Fatal(e)
				}
				if value, e := startup.NewSABLibraryPublicationIssuer(store, source); e == nil || value != nil {
					t.Fatal("closed startup issued new publication actor")
				}
			}
		})
	}
}
