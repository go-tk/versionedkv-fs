package fsstorage_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-fs/fsstorage"
	"github.com/stretchr/testify/assert"
)

func TestFSStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return makeStorage()
	})
}

func TestRedisStorage_Close(t *testing.T) {
	s, err := makeStorage()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	time.AfterFunc(1*time.Second, func() {
		s.Close() // WaitForValue should fail with error ErrStorageClosed
	})
	_, _, err = s.WaitForValue(context.Background(), "foo", nil)
	assert.Equal(t, err, versionedkv.ErrStorageClosed)
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
