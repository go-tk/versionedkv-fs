package fsstorage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/go-tk/versionedkv"
	"github.com/go-tk/versionedkv-fs/fsstorage/internal"
	"github.com/rogpeppe/go-internal/lockedfile"
	"github.com/rs/xid"
)

func Open(baseDirName string) (versionedkv.Storage, error) {
	dirNames, err := createDirs(baseDirName)
	if err != nil {
		return nil, err
	}
	var fss fsStorage
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
	dirNames dirNames
	eventBus internal.EventBus
	closure  chan struct{}
}

func (fss *fsStorage) GetValue(_ context.Context, key string) (string, versionedkv.Version, error) {
	value, version, err := fss.doGetValue(key, "")
	return value, version2OpaqueVersion(version), err
}

func (fss *fsStorage) doGetValue(key string, oldVersion string) (string, string, error) {
	if fss.eventBus.IsClosed() {
		return "", "", versionedkv.ErrStorageClosed
	}
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return "", "", err
	}
	defer versionFile.Close()
	rawNewVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return "", "", err
	}
	if len(rawNewVersion) == 0 {
		return "", "", nil
	}
	if oldVersion != "" && string(rawNewVersion) == oldVersion {
		return "", "", nil
	}
	valueFileName := fss.valueFileName(key)
	rawValue, err := ioutil.ReadFile(valueFileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", "", err
		}
	}
	return string(rawValue), string(rawNewVersion), nil
}

func (fss *fsStorage) WaitForValue(ctx context.Context, key string, oldOpaqueVersion versionedkv.Version) (string, versionedkv.Version, error) {
	value, newVersion, err := fss.doWaitForValue(ctx, key, opaqueVersion2Version(oldOpaqueVersion))
	return value, version2OpaqueVersion(newVersion), err
}

func (fss *fsStorage) doWaitForValue(ctx context.Context, key string, oldVersion string) (string, string, error) {
	for {
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
			value, newVersion, err := fss.doGetValue(key, oldVersion)
			if err != nil {
				return "", "", err
			}
			if newVersion == "" {
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
		if newVersion == "" {
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
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer versionFile.Close()
	rawVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return "", err
	}
	if len(rawVersion) >= 1 {
		return "", nil
	}
	valueFileName := fss.valueFileName(key)
	if err := ioutil.WriteFile(valueFileName, []byte(value), 0666); err != nil {
		return "", err
	}
	version := xid.New().String()
	if _, err := versionFile.WriteString(version); err != nil {
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
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return "", err
	}
	defer versionFile.Close()
	rawVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return "", err
	}
	if len(rawVersion) == 0 {
		return "", nil
	}
	if oldVersion != "" && string(rawVersion) != oldVersion {
		return "", nil
	}
	if _, err := versionFile.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	valueFileName := fss.valueFileName(key)
	if err := ioutil.WriteFile(valueFileName, []byte(value), 0666); err != nil {
		return "", err
	}
	newVersion := xid.New().String()
	if _, err := versionFile.WriteString(newVersion); err != nil {
		return "", err
	}
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
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer versionFile.Close()
	rawVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return "", err
	}
	if len(rawVersion) == 0 {
		valueFileName := fss.valueFileName(key)
		if err := ioutil.WriteFile(valueFileName, []byte(value), 0666); err != nil {
			return "", err
		}
		version := xid.New().String()
		if _, err := versionFile.WriteString(version); err != nil {
			return "", err
		}
		return version, nil
	}
	if oldVersion != "" && string(rawVersion) != oldVersion {
		return "", nil
	}
	if _, err := versionFile.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	valueFileName := fss.valueFileName(key)
	if err := ioutil.WriteFile(valueFileName, []byte(value), 0666); err != nil {
		return "", err
	}
	newVersion := xid.New().String()
	if _, err := versionFile.WriteString(newVersion); err != nil {
		return "", err
	}
	return newVersion, nil
}

func (fss *fsStorage) DeleteValue(_ context.Context, key string, opaqueVersion versionedkv.Version) (bool, error) {
	return fss.doDeleteValue(key, opaqueVersion2Version(opaqueVersion))
}

func (fss *fsStorage) doDeleteValue(key string, version string) (bool, error) {
	if fss.eventBus.IsClosed() {
		return false, versionedkv.ErrStorageClosed
	}
	versionFileName := fss.versionFileName(key)
	versionFile, err := lockedfile.OpenFile(versionFileName, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return false, err
	}
	defer versionFile.Close()
	rawVersion, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return false, err
	}
	if len(rawVersion) == 0 {
		return false, nil
	}
	if version != "" && string(rawVersion) != version {
		return false, nil
	}
	valueFileName := fss.valueFileName(key)
	if err := os.Remove(valueFileName); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
	}
	if err := versionFile.Truncate(0); err != nil {
		return false, err
	}
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
		value, version, err := fss.doGetValue(key, "")
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

func (fss *fsStorage) valueFileName(key string) string {
	return filepath.Join(fss.dirNames.Values, key)
}

func (fss *fsStorage) versionFileName(key string) string {
	return filepath.Join(fss.dirNames.Versions, key)
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
