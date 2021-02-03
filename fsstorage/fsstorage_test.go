package fsstorage_test

import (
	"io/ioutil"
	"testing"

	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-fs/fsstorage"
)

func TestFSStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return makeStorage()
	})
}

func makeStorage() (versionedkv.Storage, error) {
	baseDirName, err := ioutil.TempDir("", "testfsstorage.*")
	if err != nil {
		return nil, err
	}
	return Open(Options{
		BaseDirName: baseDirName,
	})
}
