//go:build linux

package handlers

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestParentFileHLSOutputOpenerDeniesSymlinkFIFOAndRenamedVersion(t *testing.T) {
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	path := filepath.Join(directory, "segment000000.ts")
	if err = os.WriteFile(path, []byte("synthetic closed segment"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := parentHLSOpen(root, "segment000000.ts")
	if err != nil {
		t.Fatal(err)
	}
	before, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	if err = os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("synthetic closed segment"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if sameParentHLSFile(before, after) {
		t.Fatal("equal-sized replacement segment adopted old output identity")
	}
	if err = os.Symlink("segment000000.ts", filepath.Join(directory, "segment000001.ts")); err != nil {
		t.Fatal(err)
	}
	if file, err := parentHLSOpen(root, "segment000001.ts"); file != nil || err == nil {
		if file != nil {
			file.Close()
		}
		t.Fatal("output opener followed symlink")
	}
	if err = syscall.Mkfifo(filepath.Join(directory, "segment000002.ts"), 0600); err != nil {
		t.Fatal(err)
	}
	if file, err := parentHLSOpen(root, "segment000002.ts"); file != nil || err == nil {
		if file != nil {
			file.Close()
		}
		t.Fatal("output opener accepted blocking FIFO")
	}
}
