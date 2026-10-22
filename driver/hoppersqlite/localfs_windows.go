package hoppersqlite

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// networkFilesystem reports a UNC path or a drive Windows reports as remote,
// such as a mapped network drive.
func networkFilesystem(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(abs, `\\`) && !strings.HasPrefix(abs, `\\?\`) {
		return "a network share", nil
	}
	path, err := windows.UTF16PtrFromString(abs)
	if err != nil {
		return "", err
	}
	volume := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(path, &volume[0], uint32(len(volume))); err != nil {
		return "", err
	}
	if windows.GetDriveType(&volume[0]) == windows.DRIVE_REMOTE {
		return "a network drive", nil
	}
	return "", nil
}
