package pump

import (
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

var (
	// SegmentSizeBytes is the max threshold of binlog segment file size
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 512 * 1024 * 1024

	// ErrFileNotFound is an error represents binlog file not found
	ErrFileNotFound = errors.New("binlogger: file not found")

	// ErrFileContentCorruption represents file or directory's content is curruption for some season
	ErrFileContentCorruption = errors.New("binlogger: content is corruption")

	// ErrCRCMismatch is the error represents crc don't match
	ErrCRCMismatch = errors.New("binlogger: crc mismatch")
	crcTable       = crc32.MakeTable(crc32.Castagnoli)
)

// Binlogger is the interface that for append and read binlog
type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error)

	// batch write binlog event
	WriteTail(payload []byte) error

	// Walk reads binlog from the "from" position and sends binlogs in the streaming way
	Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) (binlog.Pos, error)

	// close the binlogger
	Close() error

	// GC recycles the old binlog file
	GC(days time.Duration, pos binlog.Pos)
}

// binlogger is a logical representation of the log storage
// it is either in read mode or append mode.
type binlogger struct {
	dir string

	// encoder encodes binlog payload into bytes, and write to file
	encoder *encoder

	// file is the lastest file in the dir
	file  *file.LockedFile
	mutex sync.Mutex
}

// CreateBinlogger creates a binlog directory, then can append binlogs
func CreateBinlogger(dirpath string) (Binlogger, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if err := file.CreateDirAll(dirpath); err != nil {
		return nil, errors.Trace(err)
	}

	p := path.Join(dirpath, fileName(0))
	f, err := file.LockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:     dirpath,
		encoder: newEncoder(f),
		file:    f,
	}

	return binlog, nil
}

//OpenBinlogger returns a binlogger for write, then it can be appended
func OpenBinlogger(dirpath string) (Binlogger, error) {
	names, err := readBinlogNames(dirpath)
	if err != nil {
		return nil, err
	}

	if !isValidBinlog(names) {
		return nil, ErrFileContentCorruption
	}

	lastFileName := names[len(names)-1]
	lastFileSuffix, err := parseBinlogName(lastFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := path.Join(dirpath, lastFileName)
	f, err := file.TryLockFile(p, os.O_WRONLY, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, errors.Trace(err)
	}

	latestFilePos.Suffix = lastFileSuffix
	latestFilePos.Offset = offset

	binlog := &binlogger{
		dir:     dirpath,
		file:    f,
		encoder: newEncoder(f),
	}

	return binlog, nil
}

//CloseBinlogger closes the binlogger
func CloseBinlogger(binlogger Binlogger) error {
	return binlogger.Close()
}

// ReadFrom reads `nums` binlogs from the given binlog position
// read all binlogs from one file then close it and open the following file
func (b *binlogger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	var ent = &binlog.Entity{}
	var ents = []binlog.Entity{}
	var index int32
	var decoder *decoder
	var first = true

	dirpath := b.dir

	if nums < 0 {
		return nil, errors.Errorf("read number must be positive")
	}

	names, err := readBinlogNames(b.dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nameIndex, ok := searchIndex(names, from.Suffix)
	if !ok {
		return nil, ErrFileNotFound
	}

	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		f, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			return ents, errors.Trace(err)
		}
		defer f.Close()

		if first {
			first = false

			size, err := f.Seek(from.Offset, os.SEEK_SET)
			if err != nil {
				return ents, errors.Trace(err)
			}

			if size < from.Offset {
				return ents, errors.Errorf("pos'offset is wrong")
			}
		}

		decoder = newDecoder(from, io.Reader(f))

		for ; index < nums; index++ {
			err = decoder.decode(ent, &binlogBuffer{})
			if err != nil {
				break
			}

			newEnt := binlog.Entity{
				Pos:     ent.Pos,
				Payload: ent.Payload,
			}
			ents = append(ents, newEnt)
		}

		if (err != nil && err != io.EOF) || index == nums {
			return ents, err
		}

		from.Suffix++
		from.Offset = 0
	}

	return ents, nil
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (b *binlogger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) (binlog.Pos, error) {
	var ent = &binlog.Entity{}
	var decoder *decoder
	var first = true

	dirpath := b.dir
	latestPos := from

	names, err := readBinlogNames(b.dir)
	if err != nil {
		return latestPos, errors.Trace(err)
	}

	nameIndex, ok := searchIndex(names, from.Suffix)
	if !ok {
		return latestPos, ErrFileNotFound
	}

	for _, name := range names[nameIndex:] {
		select {
		case <-ctx.Done():
			log.Warningf("Walk Done!")
			return latestPos, nil
		default:
		}

		p := path.Join(dirpath, name)
		f, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			return latestPos, errors.Trace(err)
		}
		defer f.Close()

		if first {
			first = false

			size, err := f.Seek(from.Offset, os.SEEK_SET)
			if err != nil {
				return latestPos, errors.Trace(err)
			}

			if size < from.Offset {
				return latestPos, errors.Errorf("pos'offset is wrong")
			}
		}

		decoder = newDecoder(from, io.Reader(f))

		for {
			select {
			case <-ctx.Done():
				log.Warningf("Walk Done!")
				return latestPos, nil
			default:
			}

			buf := binlogBufferPool.Get().(*binlogBuffer)
			err = decoder.decode(ent, buf)
			if err != nil {
				break
			}

			newEnt := binlog.Entity{
				Pos:     ent.Pos,
				Payload: ent.Payload,
			}
			latestPos = newEnt.Pos
			err := sendBinlog(newEnt)
			if err != nil {
				return latestPos, errors.Trace(err)
			}

			// calculate next position to send binlog
			// 16 means magic(4 bytes) + size(8 bytes) + crc(4 bytes)
			latestPos.Offset += int64(len(newEnt.Payload) + 16)
			binlogBufferPool.Put(buf)
		}

		if err != nil && err != io.EOF {
			return latestPos, err
		}

		from.Suffix++
		from.Offset = 0
	}

	return latestPos, nil
}

// GC recycles the old binlog file
func (b *binlogger) GC(days time.Duration, pos binlog.Pos) {
	names, err := readBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files error:", names)
		return
	}

	if len(names) == 0 {
		return
	}

	// skip the latest binlog file
	for _, name := range names[:len(names)-1] {
		fileName := path.Join(b.dir, name)
		fi, err := os.Stat(fileName)
		if err != nil {
			log.Error("GC binlog file stat error:", err)
			continue
		}

		curSuffix, err := parseBinlogName(name)
		if err != nil {
			log.Errorf("parse binlog error %v", err)
		}

		if curSuffix < pos.Suffix || time.Now().Sub(fi.ModTime()) > days {
			err := os.Remove(fileName)
			if err != nil {
				log.Error("remove old binlog file err")
				continue
			}
			log.Info("GC binlog file:", fileName)
		}
	}
}

// Writes appends the binlog
// if size of current file is bigger than SegmentSizeBytes, then rotate a new file
func (b *binlogger) WriteTail(payload []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(payload) == 0 {
		return nil
	}

	if err := b.encoder.encode(payload); err != nil {
		return errors.Trace(err)
	}

	curOffset, err := b.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return errors.Trace(err)
	}
	latestFilePos.Offset = curOffset

	if curOffset < SegmentSizeBytes {
		return nil
	}

	return b.rotate()
}

// Close closes the binlogger
func (b *binlogger) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Errorf("failed to unlock during closing file: %s", err)
		}
	}

	return nil
}

// rotate creates a new file for append binlog
func (b *binlogger) rotate() error {
	filename := fileName(b.seq() + 1)
	latestFilePos.Suffix = b.seq() + 1
	latestFilePos.Offset = 0
	fpath := path.Join(b.dir, filename)

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	if err = b.file.Close(); err != nil {
		log.Errorf("failed to unlock during closing file: %s", err)
	}
	b.file = newTail

	b.encoder = newEncoder(b.file)
	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *binlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, err := parseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", b.file.Name(), err)
	}

	return seq
}
