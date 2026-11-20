//go:build !linux && !darwin && !windows

package settingscommit

import "os"

const nativeFamily = "unsupported"

func nativeOpen(string, bool) (*os.File, error) { return nil, ErrUnsupported }
func nativeLock(*os.File, bool, bool) error     { return ErrUnsupported }
func nativeUnlock(*os.File) error               { return ErrUnsupported }
func nativeConflict(error) bool                 { return false }

func nativeReadRegular(string, bool) (*os.File, error) { return nil, ErrUnsupported }

func nativePublishDirectory(string, string) error { return ErrUnsupported }
