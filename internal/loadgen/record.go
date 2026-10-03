package loadgen

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

type ErrKind uint8

const (
	ErrNone ErrKind = iota
	ErrTimeout
	ErrRefused
	ErrReset
	ErrClosed
	ErrDropped
	ErrCanceled
	ErrOther
)

var errNames = [...]string{"none", "timeout", "refused", "reset", "closed", "dropped", "canceled", "other"}

func (k ErrKind) String() string {
	if int(k) < len(errNames) {
		return errNames[k]
	}
	return fmt.Sprintf("err%d", uint8(k))
}

func classify(err error) ErrKind {
	if err == nil {
		return ErrNone
	}
	var ne net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return ErrCanceled
	case errors.Is(err, syscall.ECONNREFUSED):
		return ErrRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return ErrReset
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrClosed
	case errors.As(err, &ne) && ne.Timeout():
		return ErrTimeout
	}
	return ErrOther
}

type Record struct {
	Scheduled int64
	Started   int64
	Connected int64
	Ended     int64
	Bytes     uint32
	Status    uint16
	Dimension uint8
	Attempts  uint8
	Err       ErrKind
}

func (r Record) Latency() time.Duration { return time.Duration(r.Ended - r.Scheduled) }

func (r Record) Lag() time.Duration { return time.Duration(r.Started - r.Scheduled) }

type Log struct {
	StartUnixNano int64
	Dimensions    []string
	Records       []Record
}

var logMagic = [4]byte{'P', 'G', 'R', 'L'}

const (
	logVersion = 1
	recordSize = 4*8 + 4 + 2 + 3
)

func WriteLog(w io.Writer, l *Log) error {
	bw := bufio.NewWriterSize(w, 1<<16)
	var hdr [4 + 2 + 8 + 2]byte
	copy(hdr[:4], logMagic[:])
	binary.LittleEndian.PutUint16(hdr[4:], logVersion)
	binary.LittleEndian.PutUint64(hdr[6:], uint64(l.StartUnixNano))
	binary.LittleEndian.PutUint16(hdr[14:], uint16(len(l.Dimensions)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	for _, d := range l.Dimensions {
		var n [2]byte
		binary.LittleEndian.PutUint16(n[:], uint16(len(d)))
		if _, err := bw.Write(n[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(d); err != nil {
			return err
		}
	}
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(l.Records)))
	if _, err := bw.Write(count[:]); err != nil {
		return err
	}
	var buf [recordSize]byte
	for _, r := range l.Records {
		binary.LittleEndian.PutUint64(buf[0:], uint64(r.Scheduled))
		binary.LittleEndian.PutUint64(buf[8:], uint64(r.Started))
		binary.LittleEndian.PutUint64(buf[16:], uint64(r.Connected))
		binary.LittleEndian.PutUint64(buf[24:], uint64(r.Ended))
		binary.LittleEndian.PutUint32(buf[32:], r.Bytes)
		binary.LittleEndian.PutUint16(buf[36:], r.Status)
		buf[38] = r.Dimension
		buf[39] = r.Attempts
		buf[40] = byte(r.Err)
		if _, err := bw.Write(buf[:]); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func ReadLog(r io.Reader) (*Log, error) {
	br := bufio.NewReaderSize(r, 1<<16)
	var hdr [4 + 2 + 8 + 2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if [4]byte(hdr[:4]) != logMagic {
		return nil, errors.New("not a request log")
	}
	if v := binary.LittleEndian.Uint16(hdr[4:]); v != logVersion {
		return nil, fmt.Errorf("unsupported log version %d", v)
	}
	l := &Log{StartUnixNano: int64(binary.LittleEndian.Uint64(hdr[6:]))}
	dims := int(binary.LittleEndian.Uint16(hdr[14:]))
	for range dims {
		var n [2]byte
		if _, err := io.ReadFull(br, n[:]); err != nil {
			return nil, fmt.Errorf("read dimension: %w", err)
		}
		name := make([]byte, binary.LittleEndian.Uint16(n[:]))
		if _, err := io.ReadFull(br, name); err != nil {
			return nil, fmt.Errorf("read dimension: %w", err)
		}
		l.Dimensions = append(l.Dimensions, string(name))
	}
	var count [8]byte
	if _, err := io.ReadFull(br, count[:]); err != nil {
		return nil, fmt.Errorf("read record count: %w", err)
	}
	n := binary.LittleEndian.Uint64(count[:])
	l.Records = make([]Record, 0, min(n, 1<<24))
	var buf [recordSize]byte
	for range n {
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return nil, fmt.Errorf("read record: %w", err)
		}
		l.Records = append(l.Records, Record{
			Scheduled: int64(binary.LittleEndian.Uint64(buf[0:])),
			Started:   int64(binary.LittleEndian.Uint64(buf[8:])),
			Connected: int64(binary.LittleEndian.Uint64(buf[16:])),
			Ended:     int64(binary.LittleEndian.Uint64(buf[24:])),
			Bytes:     binary.LittleEndian.Uint32(buf[32:]),
			Status:    binary.LittleEndian.Uint16(buf[36:]),
			Dimension: buf[38],
			Attempts:  buf[39],
			Err:       ErrKind(buf[40]),
		})
	}
	return l, nil
}
