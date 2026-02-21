package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/rs/zerolog/log"
)

// Local implements RemoteStorage and BatchDeleter for local filesystem paths
type Local struct {
	Config *config.LocalConfig
}

func (l *Local) Debug(msg string, v ...interface{}) {
	if l.Config.Debug {
		log.Info().Msgf(msg, v...)
	}
}

func (l *Local) Kind() string {
	return "LOCAL"
}

// containedPath joins basePath and key, then validates the result is within basePath.
// Returns the cleaned absolute path or an error if the key escapes the base.
func containedPath(basePath, key string) (string, error) {
	joined := filepath.Clean(filepath.Join(basePath, key))
	base := filepath.Clean(basePath)
	if !strings.HasPrefix(joined, base+string(filepath.Separator)) && joined != base {
		return "", fmt.Errorf("path %q escapes base %q", key, basePath)
	}
	return joined, nil
}

func (l *Local) Connect(ctx context.Context) error {
	if l.Config.Path == "" {
		return fmt.Errorf("local->path is required")
	}
	if err := os.MkdirAll(l.Config.Path, 0750); err != nil {
		return fmt.Errorf("can't create local path %s: %v", l.Config.Path, err)
	}
	if l.Config.ObjectDiskPath != "" {
		if err := os.MkdirAll(l.Config.ObjectDiskPath, 0750); err != nil {
			return fmt.Errorf("can't create local object_disk_path %s: %v", l.Config.ObjectDiskPath, err)
		}
	}
	l.Debug("[LOCAL_DEBUG] connected to %s", l.Config.Path)
	return nil
}

func (l *Local) Close(ctx context.Context) error {
	return nil
}

func (l *Local) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	absPath, err := containedPath(l.Config.Path, key)
	if err != nil {
		return nil, err
	}
	return l.StatFileAbsolute(ctx, absPath)
}

func (l *Local) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	stat, err := os.Stat(key)
	if err != nil {
		l.Debug("[LOCAL_DEBUG] StatFile %s return error %v", key, err)
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &localFile{
		size:         stat.Size(),
		lastModified: stat.ModTime(),
		name:         filepath.Base(key),
	}, nil
}

func (l *Local) DeleteFile(ctx context.Context, key string) error {
	l.Debug("[LOCAL_DEBUG] Delete %s", key)
	absPath, err := containedPath(l.Config.Path, key)
	if err != nil {
		return err
	}
	return os.RemoveAll(absPath)
}

func (l *Local) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	l.Debug("[LOCAL_DEBUG] DeleteFileFromObjectDiskBackup %s", key)
	absPath, err := containedPath(l.Config.ObjectDiskPath, key)
	if err != nil {
		return err
	}
	return os.RemoveAll(absPath)
}

func (l *Local) Walk(ctx context.Context, remotePath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	prefix := path.Join(l.Config.Path, remotePath)
	return l.WalkAbsolute(ctx, prefix, recursive, process)
}

func (l *Local) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, RemoteFile) error) error {
	l.Debug("[LOCAL_DEBUG] Walk %s, recursive=%v", prefix, recursive)

	if recursive {
		return filepath.Walk(prefix, func(fPath string, info os.FileInfo, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			relName, relErr := filepath.Rel(prefix, fPath)
			if relErr != nil {
				return relErr
			}
			// Skip root directory entry, same as SFTP "." filtering
			if relName == "." {
				return nil
			}
			return process(ctx, &localFile{
				size:         info.Size(),
				lastModified: info.ModTime(),
				name:         relName,
			})
		})
	}

	entries, err := os.ReadDir(prefix)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		l.Debug("[LOCAL_DEBUG] Walk::NonRecursive::ReadDir %s return error %v", prefix, err)
		return err
	}
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if err := process(ctx, &localFile{
			size:         info.Size(),
			lastModified: info.ModTime(),
			name:         entry.Name(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *Local) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	absPath, err := containedPath(l.Config.Path, key)
	if err != nil {
		return nil, err
	}
	return l.GetFileReaderAbsolute(ctx, absPath)
}

func (l *Local) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(key)
}

func (l *Local) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return l.GetFileReader(ctx, key)
}

func (l *Local) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	absPath, err := containedPath(l.Config.Path, key)
	if err != nil {
		return err
	}
	return l.PutFileAbsolute(ctx, absPath, r, localSize)
}

func (l *Local) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	dir := filepath.Dir(key)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("can't create directory %s: %v", dir, err)
	}
	dst, err := os.Create(key)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := dst.Close(); closeErr != nil {
			log.Warn().Msgf("can't close %s err=%v", key, closeErr)
		}
	}()
	if _, err = io.Copy(dst, r); err != nil {
		// Clean up partial file on copy failure
		if removeErr := os.Remove(key); removeErr != nil {
			log.Warn().Msgf("can't remove partial file %s err=%v", key, removeErr)
		}
		return err
	}
	return nil
}

func (l *Local) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	// Prefix dstKey with ObjectDiskPath, consistent with S3/GCS/AzureBlob
	dstKey = path.Join(l.Config.ObjectDiskPath, dstKey)
	srcPath := path.Join(l.Config.Path, srcKey)
	dstPath := dstKey

	l.Debug("[LOCAL_DEBUG] CopyObject %s -> %s", srcPath, dstPath)

	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0750); err != nil {
		return 0, fmt.Errorf("can't create directory %s: %v", dstDir, err)
	}

	// Try hardlink first
	if err := os.Link(srcPath, dstPath); err == nil {
		l.Debug("[LOCAL_DEBUG] CopyObject hardlink %s -> %s", srcPath, dstPath)
		return srcSize, nil
	}

	// Fallback to file copy
	l.Debug("[LOCAL_DEBUG] CopyObject copy %s -> %s", srcPath, dstPath)
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			log.Warn().Msgf("can't close %s err=%v", srcPath, closeErr)
		}
	}()
	dst, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := dst.Close(); closeErr != nil {
			log.Warn().Msgf("can't close %s err=%v", dstPath, closeErr)
		}
	}()
	written, err := io.Copy(dst, src)
	if err != nil {
		return 0, err
	}
	return written, nil
}

// DeleteKeysBatch implements BatchDeleter interface for Local
func (l *Local) DeleteKeysBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	l.Debug("[LOCAL_DEBUG] DeleteKeysBatch: deleting %d keys", len(keys))
	return l.deleteKeysBatchInternal(ctx, l.Config.Path, keys)
}

// DeleteKeysFromObjectDiskBackupBatch implements BatchDeleter interface for Local
func (l *Local) DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	l.Debug("[LOCAL_DEBUG] DeleteKeysFromObjectDiskBackupBatch: deleting %d keys", len(keys))
	return l.deleteKeysBatchInternal(ctx, l.Config.ObjectDiskPath, keys)
}

func (l *Local) deleteKeysBatchInternal(ctx context.Context, basePath string, keys []string) error {
	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		absPath, pathErr := containedPath(basePath, key)
		if pathErr != nil {
			failures = append(failures, KeyError{Key: key, Err: pathErr})
			continue
		}
		if err := os.RemoveAll(absPath); err != nil {
			failures = append(failures, KeyError{Key: key, Err: err})
			continue
		}
		deletedCount++
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("LOCAL batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("LOCAL batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

// localFile implements RemoteFile
type localFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *localFile) Size() int64 {
	return f.size
}

func (f *localFile) LastModified() time.Time {
	return f.lastModified
}

func (f *localFile) Name() string {
	return f.name
}

// Compile-time interface checks
var _ RemoteStorage = &Local{}
var _ BatchDeleter = &Local{}

// Ensure localFile implements RemoteFile
var _ RemoteFile = &localFile{}
