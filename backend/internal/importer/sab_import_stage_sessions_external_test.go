package importer_test

import (
	"time"

	"novastream/internal/importer"
	"novastream/services/sessions"
)

var _ importer.StageTestSessionService = (*sessions.Service)(nil)

func init() {
	if importer.StageTestSessionFactory != nil {
		panic("stage session fixture constructor already initialized")
	}
	importer.StageTestSessionFactory = func(storageDir string, duration time.Duration) (importer.StageTestSessionService, error) {
		return sessions.NewService(storageDir, duration)
	}
}
