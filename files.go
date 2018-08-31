package ds_to_json

import (
	"os"
	"path/filepath"
	"strings"
)

// This is a syncronous crawler that will go through a directory
// and returns a list of files
func WalkDir(root string) ([]string, error) {
	filesList := make([]string, 0)

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		baseFilename := filepath.Base(path)

		if !strings.HasPrefix(baseFilename, "output") {
			return nil
		}

		filesList = append(filesList, path)

		return nil

	})

	if err != nil {
		return nil, err
	}

	return filesList, nil
}

// Deletes all files & folders in a directory
func CleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// Ensures a directory is created for writing files
func EnsureDir(dir string) {
	os.MkdirAll(dir, os.ModePerm)
}
