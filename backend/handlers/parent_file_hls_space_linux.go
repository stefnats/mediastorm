//go:build linux

package handlers

import (
	"math"
	"os"
	"syscall"
)

// Admission evidence only; this does not reserve blocks from other processes.
func parentHLSFreeSpace(directory *os.File, required int64) bool {
	if directory == nil || required <= 0 {
		return false
	}
	var stat syscall.Statfs_t
	if syscall.Fstatfs(int(directory.Fd()), &stat) != nil || stat.Bsize <= 0 || stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return false
	}
	return stat.Bavail*uint64(stat.Bsize) >= uint64(required)+(64<<20)
}
func parentHLSOpen(root *os.Root, name string) (*os.File, error) {
	named, err := root.Lstat(name)
	if err != nil || !named.Mode().IsRegular() {
		return nil, errParentFileHLS
	}
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errParentFileHLS
	}
	actual, err := file.Stat()
	after, e := root.Lstat(name)
	if err != nil || e != nil || !sameParentHLSFile(named, actual) || !sameParentHLSFile(actual, after) {
		file.Close()
		return nil, errParentFileHLS
	}
	return file, nil
}
