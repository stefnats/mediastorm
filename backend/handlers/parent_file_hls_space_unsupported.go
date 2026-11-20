//go:build !linux

package handlers

import "os"

func parentHLSFreeSpace(*os.File, int64) bool          { return false }
func parentHLSOpen(*os.Root, string) (*os.File, error) { return nil, errParentFileHLS }
