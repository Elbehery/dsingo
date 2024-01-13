package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mux  sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	pos = s.size
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w = lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	p := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(p, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return p, nil
}

func (s *store) ReadAt(p []byte, pos int64) (int, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, nil
	}
	return s.File.ReadAt(p, pos)
}

func (s *store) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
