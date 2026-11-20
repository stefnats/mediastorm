//go:build linux

package config

import (
	"context"
	"encoding/json"
	"io"
	"novastream/internal/settingscommit"
	"os"
	"path/filepath"
)

func recordingPolicySupported() bool { return true }
func readOriginalSecurityPolicy(ctx context.Context, manager *Manager) (bool, os.FileInfo, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil {
		return false, nil, ErrSecurityCoordinator
	}
	absolute, err := filepath.Abs(manager.path)
	if err != nil {
		return false, nil, err
	}
	for path := absolute; ; path = filepath.Dir(path) {
		info, e := os.Lstat(path)
		if e != nil || info.Mode()&os.ModeSymlink != 0 {
			return false, nil, ErrSecurityCoordinator
		}
		if filepath.Dir(path) == path {
			break
		}
	}
	file, err := settingscommit.OpenRegularGuardData(manager.path)
	if err != nil {
		return false, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	named, e := os.Lstat(manager.path)
	if err != nil || e != nil || !info.Mode().IsRegular() || !os.SameFile(info, named) || info.Size() > 4<<20 {
		return false, nil, ErrSecurityCoordinator
	}
	var data struct {
		Security SecuritySettings `json:"security"`
	}
	decoder := json.NewDecoder(io.LimitReader(file, (4<<20)+1))
	if decoder.Decode(&data) != nil {
		return false, nil, ErrSecurityCoordinator
	}
	var tail any
	if decoder.Decode(&tail) != io.EOF || ctx.Err() != nil {
		return false, nil, ErrSecurityCoordinator
	}
	return data.Security.EnforceProfilePins, info, nil
}
