package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/filestream"
)

var tmpFileNum uint64

// MustSyncPath syncs contents of the given path.
func MustSyncPath(path string) {
	mustSyncPath(path)
}

// WriteFileAtomically atomically writes data to the given file path.
//
// WriteFileAtomically returns only after the file is fully written and synced
// to the underlying storage.
func WriteFileAtomically(path string, lockPath *string, data []byte) error {
	// Check for the existing file. It is expected that
	// the WriteFileAtomically function cannot be called concurrently
	// with the same `path`.
	if IsPathExist(path) {
		return fmt.Errorf("cannot create file %q, since it already exists", path)
	}

	n := atomic.AddUint64(&tmpFileNum, 1)
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, n)
	f, err := filestream.Create(tmpPath, lockPath, false)
	if err != nil {
		return fmt.Errorf("cannot create file %q: %w", tmpPath, err)
	}
	if _, err := f.Write(data); err != nil {
		f.MustClose()
		MustRemoveAll(tmpPath, lockPath)
		return fmt.Errorf("cannot write %d bytes to file %q: %w", len(data), tmpPath, err)
	}

	// Sync and close the file.
	f.MustClose()

	// Atomically move the file from tmpPath to path.
	lock := fileops.FileLockOption(*lockPath)
	if err := fileops.RenameFile(tmpPath, path, lock); err != nil {
		// do not call MustRemoveAll(tmpPath) here, so the user could inspect
		// the file contents during investigating the issue.
		return fmt.Errorf("cannot move %q to %q: %w", tmpPath, path, err)
	}

	// Sync the containing directory, so the file is guaranteed to appear in the directory.
	// See https://www.quora.com/When-should-you-fsync-the-containing-directory-in-addition-to-the-file-itself

	if fileops.GetFsType(path) != fileops.Obs {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("cannot obtain absolute path to %q: %w", path, err)
		}
		parentDirPath := filepath.Dir(absPath)
		MustSyncPath(parentDirPath)
	}

	return nil
}

// IsTemporaryFileName returns true if fn matches temporary file name pattern
// from WriteFileAtomically.
func IsTemporaryFileName(fn string) bool {
	return tmpFileNameRe.MatchString(fn)
}

// tmpFileNameRe is regexp for temporary file name - see WriteFileAtomically for details.
var tmpFileNameRe = regexp.MustCompile(`\.tmp\.\d+$`)

// MkdirAllIfNotExist creates the given path dir if it isn't exist.
func MkdirAllIfNotExist(path string, lock *string) error {
	if IsPathExist(path) {
		return nil
	}
	return mkdirSync(path, lock)
}

// MkdirAllFailIfExist creates the given path dir if it isn't exist.
//
// Returns error if path already exists.
func MkdirAllFailIfExist(path string, lock *string) error {
	if IsPathExist(path) {
		return fmt.Errorf("the %q already exists", path)
	}
	return mkdirSync(path, lock)
}

func mkdirSync(path string, lockPath *string) error {
	lock := fileops.FileLockOption(*lockPath)
	if err := fileops.MkdirAll(path, 0755, lock); err != nil {
		return err
	}
	// Sync the parent directory, so the created directory becomes visible
	// in the fs after power loss.
	parentDirPath := filepath.Dir(path)
	MustSyncPath(parentDirPath)
	return nil
}

// RemoveDirContents removes all the contents of the given dir if it exists.
//
// It doesn't remove the dir itself, so the dir may be mounted
// to a separate partition.
func RemoveDirContents(dir string, lock *string) {
	if !IsPathExist(dir) {
		// The path doesn't exist, so nothing to remove.
		return
	}
	fis, err := fileops.ReadDir(dir)
	if err != nil {
		logger.Panicf("FATAL: cannot read contents of the dir %q: %s", dir, err)
	}

	for _, fi := range fis {
		if fi.Name() == "." || fi.Name() == ".." || fi.Name() == "lost+found" {
			// Skip special dirs.
			continue
		}
		fullPath := dir + "/" + fi.Name()
		MustRemoveAll(fullPath, lock)
	}
	MustSyncPath(dir)
}

// MustClose must close the given file f.
func MustClose(f fileops.File) {
	if f == nil {
		return
	}
	fname := f.Name()
	if err := f.Close(); err != nil {
		logger.Panicf("FATAL: cannot close %q: %s", fname, err)
	}
}

// MustFileSize returns file size for the given path.
func MustFileSize(path string) uint64 {
	fi, err := fileops.Stat(path)
	if err != nil {
		logger.Panicf("FATAL: cannot stat %q: %s", path, err)
	}
	if fi.IsDir() {
		logger.Panicf("FATAL: %q must be a file, not a directory", path)
	}
	return uint64(fi.Size())
}

// IsPathExist returns whether the given path exists.
func IsPathExist(path string) bool {
	if _, err := fileops.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		logger.Panicf("FATAL: cannot stat %q: %s", path, err)
	}
	return true
}

func mustSyncParentDirIfExists(path string) {
	parentDirPath := filepath.Dir(path)
	if !IsPathExist(parentDirPath) {
		return
	}
	MustSyncPath(parentDirPath)
}

// IsEmptyDir returns true if path points to empty directory.
func IsEmptyDir(path string) bool {
	fis, err := fileops.ReadDir(path)
	if err != nil {
		if err == io.EOF {
			return true
		}
		logger.Panicf("FATAL: unexpected error when reading directory %q: %s", path, err)
	}
	if len(fis) == 0 {
		return true
	}
	return false
}

// MustRemoveAll removes path with all the contents.
//
// It properly fsyncs the parent directory after path removal.
//
// It properly handles NFS issue https://github.com/VictoriaMetrics/VictoriaMetrics/issues/61 .
func MustRemoveAll(path string, lock *string) {
	mustRemoveAll(path, lock, func() {})
}

// MustRemoveAllWithDoneCallback removes path with all the contents.
//
// It properly fsyncs the parent directory after path removal.
//
// done is called after the path is successfully removed.
//
// done may be called after the function returns for NFS path.
// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/61.
func MustRemoveAllWithDoneCallback(path string, lock *string, done func()) {
	mustRemoveAll(path, lock, done)
}

// HardLinkFiles makes hard links for all the files from srcDir in dstDir.
func HardLinkFiles(srcDir, dstDir string) error {
	lockPath := ""
	if err := mkdirSync(dstDir, &lockPath); err != nil {
		return fmt.Errorf("cannot create dstDir=%q: %w", dstDir, err)
	}

	d, err := os.Open(filepath.Clean(srcDir))
	if err != nil {
		return fmt.Errorf("cannot open srcDir=%q: %w", srcDir, err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			logger.Panicf("FATAL: cannot close %q: %s", srcDir, err)
		}
	}()

	fis, err := d.Readdir(-1)
	if err != nil {
		return fmt.Errorf("cannot read files in scrDir=%q: %w", srcDir, err)
	}
	for _, fi := range fis {
		if IsDirOrSymlink(fi) {
			// Skip directories.
			continue
		}
		fn := fi.Name()
		srcPath := srcDir + "/" + fn
		dstPath := dstDir + "/" + fn
		if err := os.Link(srcPath, dstPath); err != nil {
			return err
		}
	}

	MustSyncPath(dstDir)
	return nil
}

// IsDirOrSymlink returns true if fi is directory or symlink.
func IsDirOrSymlink(fi os.FileInfo) bool {
	return fi.IsDir() || (fi.Mode()&os.ModeSymlink == os.ModeSymlink)
}

// SymlinkRelative creates relative symlink for srcPath in dstPath.
func SymlinkRelative(srcPath, dstPath string) error {
	baseDir := filepath.Dir(dstPath)
	srcPathRel, err := filepath.Rel(baseDir, srcPath)
	if err != nil {
		return fmt.Errorf("cannot make relative path for srcPath=%q: %w", srcPath, err)
	}
	return os.Symlink(srcPathRel, dstPath)
}

// CopyDirectory copies all the files in srcPath to dstPath.
func CopyDirectory(srcPath, dstPath string, lock *string) error {
	fis, err := fileops.ReadDir(srcPath)
	if err != nil {
		return err
	}
	if err := MkdirAllIfNotExist(dstPath, lock); err != nil {
		return err
	}
	for _, fi := range fis {
		if !fi.Mode().IsRegular() {
			// Skip non-files
			continue
		}
		src := filepath.Join(srcPath, fi.Name())
		dst := filepath.Join(dstPath, fi.Name())
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	MustSyncPath(dstPath)
	return nil
}

func copyFile(srcPath, dstPath string) error {
	src, err := fileops.Open(srcPath)
	if err != nil {
		return err
	}
	defer MustClose(src)
	dst, err := fileops.Create(dstPath)
	if err != nil {
		return err
	}
	defer MustClose(dst)
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	MustSyncPath(dstPath)
	return nil
}

// ReadFullData reads len(data) bytes from r.
func ReadFullData(r io.Reader, data []byte) error {
	n, err := io.ReadFull(r, data)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("cannot read %d bytes; read only %d bytes; error: %w", len(data), n, err)
	}
	if n != len(data) {
		logger.Panicf("BUG: io.ReadFull read only %d bytes; must read %d bytes", n, len(data))
	}
	return nil
}

// MustWriteData writes data to w.
func MustWriteData(w io.Writer, data []byte) {
	if len(data) == 0 {
		return
	}
	n, err := w.Write(data)
	if err != nil {
		logger.Panicf("FATAL: cannot write %d bytes: %s", len(data), err)
	}
	if n != len(data) {
		logger.Panicf("BUG: writer wrote %d bytes instead of %d bytes", n, len(data))
	}
}

// MustGetFreeSpace returns free space for the given directory path.
func MustGetFreeSpace(path string) uint64 {
	// Try obtaining cached value at first.
	freeSpaceMapLock.Lock()
	defer freeSpaceMapLock.Unlock()

	e, ok := freeSpaceMap[path]
	if ok && fasttime.UnixTimestamp()-e.updateTime < 2 {
		// Fast path - the entry is fresh.
		return e.freeSpace
	}

	// Slow path.
	// Determine the amount of free space at path.
	e.freeSpace = mustGetFreeSpace(path)
	e.updateTime = fasttime.UnixTimestamp()
	freeSpaceMap[path] = e
	return e.freeSpace
}

var (
	freeSpaceMap     = make(map[string]freeSpaceEntry)
	freeSpaceMapLock sync.Mutex
)

type freeSpaceEntry struct {
	updateTime uint64
	freeSpace  uint64
}
