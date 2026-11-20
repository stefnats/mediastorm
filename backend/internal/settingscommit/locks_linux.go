//go:build linux

package settingscommit

import (
	"golang.org/x/sys/unix"
	"os"
)

const nativeFamily = "unix-flock-v1"

func nativeOpen(path string, create bool) (*os.File, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Open(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, ErrMetadata
	}
	return file, nil
}
func nativeLock(file *os.File, shared, nonblocking bool) error {
	operation := unix.LOCK_EX
	if shared {
		operation = unix.LOCK_SH
	}
	if nonblocking {
		operation |= unix.LOCK_NB
	}
	return unix.Flock(int(file.Fd()), operation)
}
func nativeUnlock(file *os.File) error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }
func nativeConflict(err error) bool    { return err == unix.EWOULDBLOCK || err == unix.EAGAIN }

func nativeReadRegular(path string, noFollow bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK
	if noFollow {
		flags |= unix.O_NOFOLLOW
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}
