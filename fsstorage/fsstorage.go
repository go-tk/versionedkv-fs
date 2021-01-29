// Package fsstorage provides the implementation of versionedkv based on the file system.
package fsstorage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-tk/versionedkv"
	"github.com/go-tk/versionedkv-fs/fsstorage/internal"
	"github.com/rogpeppe/go-internal/lockedfile"
	"github.com/rs/xid"
)

// Options represents options for file system storages.
type Options struct {
	BaseDirName string
}

func (o *Options) sanitize() {
	if o.BaseDirName == "" {
		o.BaseDirName = "versionedkv"
	}
}

// Open creates a new file system storage with the given options.
func Open(options Options) (versionedkv.Storage, error) {
	var fss fsStorage
	fss.options = options
	fss.options.sanitize()
	dirNames, err := createDirs(fss.options.BaseDirName)
	if err != nil {
		return nil, err
	}
	fss.dirNames = dirNames
	fss.eventBus.Init(internal.EventBusOptions{
		EventDirName: dirNames.Versions,
	})
	if err := fss.eventBus.Open(); err != nil {
		return nil, err
	}
	fss.closure = make(chan struct{})
	return &fss, nil
}

type fsStorage struct {
	options  Options
	dirNames dirNames
	eventBus internal.EventBus
	closure  chan struct{}
}

func (fss *fsStorage) GetValue(_ context.Context, key string) (string, versionedkv.Version, error) {
	value, version, _, err := fss.doGetValue(key, "")
	return value, version2OpaqueVersion(version), err
}

func (fss *fsStorage) doGetValue(key string, oldVersion string) (string, string, bool, error) {
	if fss.eventBus.IsClosed() {
		return "", "", false, versionedkv.ErrStorageClosed
	}
	versionFile, newVersion, err := fss.openAndReadVersionFile(key, os.O_RDONLY)
	if err == nil {
		defer versionFile.Close()
	} else {
		if !os.IsNotExist(err) {
			return "", "", false, nil
		}
	}
	if newVersion == oldVersion {
		return "", "", false, nil
	}
	if newVersion == "" {
		return "", "", true, nil
	}
	valueFileName := fss.valueFileName(key, newVersion)
	rawValue, err := ioutil.ReadFile(valueFileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", "", false, err
		}
	}
	return string(rawValue), newVersion, true, nil
}

func (fss *fsStorage) WaitForValue(ctx context.Context, key string, oldOpaqueVersion versionedkv.Version) (string, versionedkv.Version, error) {
	value, newVersion, err := fss.doWaitForValue(ctx, key, opaqueVersion2Version(oldOpaqueVersion))
	return value, version2OpaqueVersion(newVersion), err
}

func (fss *fsStorage) doWaitForValue(ctx context.Context, key string, oldVersion string) (string, string, error) {
	for {
		var retry bool
		value, newVersion, err := func() (string, string, error) {
			watcher, err := fss.eventBus.AddWatcher(key)
			if err != nil {
				if err == internal.ErrEventBusClosed {
					err = versionedkv.ErrStorageClosed
				}
				return "", "", err
			}
			defer func() {
				if watcher != (internal.Watcher{}) {
					fss.eventBus.RemoveWatcher(key, watcher)
				}
			}()
			value, newVersion, ok, err := fss.doGetValue(key, oldVersion)
			if err != nil {
				return "", "", err
			}
			retry = !ok
			if retry {
				select {
				case <-watcher.Event():
					watcher = internal.Watcher{}
					return "", "", nil
				case <-fss.closure:
					watcher = internal.Watcher{}
					return "", "", versionedkv.ErrStorageClosed
				case <-ctx.Done():
					return "", "", ctx.Err()
				}
			}
			return value, newVersion, nil
		}()
		if err != nil {
			return "", "", err
		}
		if retry {
			continue
		}
		return value, newVersion, nil
	}
}

func (fss *fsStorage) CreateValue(_ context.Context, key string, value string) (versionedkv.Version, error) {
	version, err := fss.doCreateValue(key, value)
	return version2OpaqueVersion(version), err
}

func (fss *fsStorage) doCreateValue(key string, value string) (string, error) {
	if fss.eventBus.IsClosed() {
		return "", versionedkv.ErrStorageClosed
	}
	versionFile, currentVersion, err := fss.openAndReadVersionFile(key, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return "", err
	}
	defer versionFile.Close()
	if currentVersion != "" {
		return "", nil
	}
	version := xid.New().String()
	if err := fss.setValue(key, value, version, versionFile); err != nil {
		return "", err
	}
	return version, nil
}

func (fss *fsStorage) UpdateValue(_ context.Context, key string, value string, opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	newVersion, err := fss.doUpdateValue(key, value, opaqueVersion2Version(opaqueOldVersion))
	return version2OpaqueVersion(newVersion), err
}

func (fss *fsStorage) doUpdateValue(key string, value string, oldVersion string) (string, error) {
	if fss.eventBus.IsClosed() {
		return "", versionedkv.ErrStorageClosed
	}
	versionFile, currentVersion, err := fss.openAndReadVersionFile(key, os.O_RDWR)
	if err == nil {
		defer versionFile.Close()
	} else {
		if !os.IsNotExist(err) {
			return "", err
		}
	}
	if currentVersion == "" {
		return "", nil
	}
	if oldVersion != "" && currentVersion != oldVersion {
		return "", nil
	}
	newVersion := xid.New().String()
	if err := fss.setValue(key, value, newVersion, versionFile); err != nil {
		return "", err
	}
	valueFileName := fss.valueFileName(key, currentVersion)
	os.Remove(valueFileName)
	return newVersion, nil
}

func (fss *fsStorage) CreateOrUpdateValue(_ context.Context, key string, value string, opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	newVersion, err := fss.doCreateOrUpdateValue(key, value, opaqueVersion2Version(opaqueOldVersion))
	return version2OpaqueVersion(newVersion), err
}

func (fss *fsStorage) doCreateOrUpdateValue(key string, value string, oldVersion string) (string, error) {
	if fss.eventBus.IsClosed() {
		return "", versionedkv.ErrStorageClosed
	}
	versionFile, currentVersion, err := fss.openAndReadVersionFile(key, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return "", err
	}
	defer versionFile.Close()
	if currentVersion == "" {
		version := xid.New().String()
		if err := fss.setValue(key, value, version, versionFile); err != nil {
			return "", err
		}
		return version, nil
	}
	if oldVersion != "" && currentVersion != oldVersion {
		return "", nil
	}
	newVersion := xid.New().String()
	if err := fss.setValue(key, value, newVersion, versionFile); err != nil {
		return "", err
	}
	valueFileName := fss.valueFileName(key, currentVersion)
	os.Remove(valueFileName)
	return newVersion, nil
}

func (fss *fsStorage) DeleteValue(_ context.Context, key string, opaqueVersion versionedkv.Version) (bool, error) {
	return fss.doDeleteValue(key, opaqueVersion2Version(opaqueVersion))
}

func (fss *fsStorage) doDeleteValue(key string, version string) (bool, error) {
	if fss.eventBus.IsClosed() {
		return false, versionedkv.ErrStorageClosed
	}
	versionFile, currentVersion, err := fss.openAndReadVersionFile(key, os.O_RDWR)
	if err == nil {
		defer versionFile.Close()
	} else {
		if !os.IsNotExist(err) {
			return false, err
		}
	}
	if currentVersion == "" {
		return false, nil
	}
	if version != "" && currentVersion != version {
		return false, nil
	}
	if runtime.GOOS == "darwin" {
		if _, err := versionFile.Write([]byte{0}); err != nil {
			return false, err
		}
	}
	if err := versionFile.Truncate(0); err != nil {
		return false, err
	}
	valueFileName := fss.valueFileName(key, currentVersion)
	os.Remove(valueFileName)
	return true, nil
}

func (fss *fsStorage) Close() error {
	err := fss.eventBus.Close()
	if err == internal.ErrEventBusClosed {
		return versionedkv.ErrStorageClosed
	}
	close(fss.closure)
	return nil
}

func (fss *fsStorage) Inspect(_ context.Context) (versionedkv.StorageDetails, error) {
	if fss.eventBus.IsClosed() {
		return versionedkv.StorageDetails{IsClosed: true}, nil
	}
	fileInfos, err := ioutil.ReadDir(fss.dirNames.Versions)
	if err != nil {
		return versionedkv.StorageDetails{}, err
	}
	var valueDetails map[string]versionedkv.ValueDetails
	for _, fileInfo := range fileInfos {
		key := fileInfo.Name()
		value, version, _, err := fss.doGetValue(key, "")
		if err != nil {
			return versionedkv.StorageDetails{}, err
		}
		if version == "" {
			continue
		}
		if valueDetails == nil {
			valueDetails = make(map[string]versionedkv.ValueDetails)
		}
		valueDetails[key] = versionedkv.ValueDetails{
			V:       value,
			Version: version,
		}
	}
	return versionedkv.StorageDetails{
		Values: valueDetails,
	}, nil
}

func (fss *fsStorage) valueFileName(key, version string) string {
	return filepath.Join(fss.dirNames.Values, key+"."+version)
}

func (fss *fsStorage) versionFileName(key string) string {
	return filepath.Join(fss.dirNames.Versions, key)
}

func (fss *fsStorage) openAndReadVersionFile(key string, flag int) (*lockedfile.File, string, error) {
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, flag, 0666)
	if err != nil {
		return nil, "", err
	}
	rawVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		versionFile.Close()
		return nil, "", err
	}
	if len(rawVersion) >= 1 {
		if _, err := versionFile.Seek(0, io.SeekStart); err != nil {
			versionFile.Close()
			return nil, "", err
		}
	}
	return versionFile, string(rawVersion), nil
}

func (fss *fsStorage) setValue(key, value, version string, versionFile *lockedfile.File) error {
	valueFileName := fss.valueFileName(key, version)
	if err := ioutil.WriteFile(valueFileName, []byte(value), 0666); err != nil {
		return err
	}
	if _, err := versionFile.WriteString(version); err != nil {
		return err
	}
	return nil
}

type dirNames struct {
	Values   string
	Versions string
}

func createDirs(baseDirName string) (dirNames, error) {
	valuesDirName := filepath.Join(baseDirName, "values")
	if err := os.MkdirAll(valuesDirName, os.ModePerm); err != nil {
		return dirNames{}, err
	}
	versionsDirName := filepath.Join(baseDirName, "versions")
	if err := os.MkdirAll(versionsDirName, os.ModePerm); err != nil {
		return dirNames{}, err
	}
	return dirNames{
		Values:   valuesDirName,
		Versions: versionsDirName,
	}, nil
}

func version2OpaqueVersion(version string) versionedkv.Version {
	if version == "" {
		return nil
	}
	return version
}

func opaqueVersion2Version(opaqueVersion versionedkv.Version) string {
	if opaqueVersion == nil {
		return ""
	}
	return opaqueVersion.(string)
}
