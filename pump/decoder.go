package pump

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type decoder struct {
	br  *bufio.Reader
	pos pb.Pos
}

func newDecoder(pos pb.Pos, r io.Reader) *decoder {
	reader := bufio.NewReader(r)

	return &decoder{
		br:  reader,
		pos: pos,
	}
}

func (d *decoder) decode(ent *pb.Binlog) error {
	if d.br == nil {
		return io.EOF
	}

	// read and chekc magic number
	magicNum, err := readInt32(d.br)
	if err == io.EOF {
		d.br = nil
		return io.EOF
	}

	err = checkMagic(magicNum)
	if err != nil {
		return err
	}

	// read payload+crc  length
	size, err := readInt64(d.br)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	data := make([]byte, size+4)

	// read payload+crc
	if _, err = io.ReadFull(d.br, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	// decode bytes to ent struct and validate crc
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	ent.Payload = data[:size]
	crc := crc32.Checksum(ent.Payload, crcTable)
	if crc != entryCrc {
		return ErrCRCMismatch
	}

	ent.Pos = pb.Pos{
		Suffix: d.pos.Suffix,
		Offset: d.pos.Offset,
	}

	// 12 is size + magic length
	d.pos.Offset += size + 16

	return nil
}

func checkMagic(mgicNum uint32) error {
	if mgicNum != magic {
		return ErrCRCMismatch
	}

	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
