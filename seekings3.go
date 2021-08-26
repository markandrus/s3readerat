package seekinghttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// SeekingS3 uses a series of HTTP GETs with Range headers
// to implement io.ReadSeeker and io.ReaderAt.
type SeekingS3 struct {
	Debug      bool
	client     *s3.Client
	bucket     string
	key        string
	offset     int64
	last       *bytes.Buffer
	lastOffset int64
	size       int64
}

// Compile-time check of interface implementations.
var _ io.ReadSeeker = (*SeekingS3)(nil)
var _ io.ReaderAt = (*SeekingS3)(nil)

// New initializes a SeekingS3 for the given URL.
// The SeekingS3.Client field may be set before the first call
// to Read or Seek.
func New(client *s3.Client, bucket string, key string) *SeekingS3 {
	return &SeekingS3{
		client: client,
		bucket: bucket,
		key:    key,
		offset: 0,
		size:   -1,
	}
}

func fmtRange(from, l int64) string {
	var to int64
	if l == 0 {
		to = from
	} else {
		to = from + (l - 1)
	}
	r := fmt.Sprintf("bytes=%v-%v", from, to)
	return r
}

// ReadAt reads len(buf) bytes into buf starting at offset off.
func (s *SeekingS3) ReadAt(buf []byte, off int64) (int, error) {
	if s.Debug {
		log.Printf("ReadAt len %v off %v", len(buf), off)
	}

	size, err := s.Size()
	if err != nil {
		return 0, err
	}

	if off >= size {
		return 0, io.EOF
	}

	if s.last != nil && off > s.lastOffset {
		end := off + int64(len(buf))
		if end < s.lastOffset+int64(s.last.Len()) {
			start := off - s.lastOffset
			if s.Debug {
				log.Printf("cache hit: range (%v-%v) is within cache (%v-%v)", off, off+int64(len(buf)), s.lastOffset, s.lastOffset+int64(s.last.Len()))
			}
			copy(buf, s.last.Bytes()[start:end-s.lastOffset])
			return len(buf), nil
		}
	}

	if s.last != nil {
		if s.Debug {
			log.Printf("cache miss: range (%v-%v) is NOT within cache (%v-%v)", off, off+int64(len(buf)), s.lastOffset, s.lastOffset+int64(s.last.Len()))
		}
	} else {
		if s.Debug {
			log.Printf("cache miss: cache empty")
		}
	}

	bytesToRead := len(buf)
	if off+int64(bytesToRead) > size {
		bytesToRead = int(size - off)
	}

	// Fetch more than what they asked for to reduce round-trips
	wanted := bytesToRead // len(buf) // 10 * len(buf)
	rng := fmtRange(off, int64(wanted))

	if s.last == nil {
		// Cache does not exist yet. So make it.
		s.last = &bytes.Buffer{}
	} else {
		// Cache is getting replaced. Bring it back to zero bytes, but
		// keep the underlying []byte, since we'll reuse it right away.
		s.last.Reset()
	}

	if s.Debug {
		log.Println("Start HTTP GET with Range:", rng)
	}

	resp, err := s.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Range:  &rng,
	})
	if err != nil {
		return 0, err
	}

	if s.Debug {
		log.Println("HTTP ok.")
	}
	s.last.ReadFrom(resp.Body)
	resp.Body.Close()
	s.lastOffset = off
	var n int
	if s.last.Len() < len(buf) {
		n = s.last.Len()
		copy(buf, s.last.Bytes()[0:n])
	} else {
		n = len(buf)
		copy(buf, s.last.Bytes())
	}

	// HTTP is trying to tell us, "that's all". Which is fine, but we don't
	// want callers to think it is EOF, it's not.
	if err == io.EOF && n == len(buf) {
		err = nil
	}
	return int(bytesToRead), err
}

func (s *SeekingS3) Read(buf []byte) (int, error) {
	if s.Debug {
		log.Printf("got read len %v", len(buf))
	}
	n, err := s.ReadAt(buf, s.offset)
	if err == nil {
		s.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read.
func (s *SeekingS3) Seek(offset int64, whence int) (int64, error) {
	if s.Debug {
		log.Printf("got seek %v %v", offset, whence)
	}
	switch whence {
	case io.SeekStart:
		s.offset = offset
		if s.Debug {
			log.Printf("offset is now %d", s.offset)
		}
	case io.SeekCurrent:
		s.offset += offset
		if s.Debug {
			log.Printf("offset is now %d", s.offset)
		}
	case io.SeekEnd:
		size, err := s.Size()
		if err != nil {
			return 0, err
		}
		// NOTE(mroberts): What should seeking beyond the end of the file do?
		s.offset = size + offset
		if s.Debug {
			log.Printf("offset is now %d", s.offset)
		}
	default:
		return 0, os.ErrInvalid
	}
	return s.offset, nil
}

// Size uses an HTTP HEAD to find out how many bytes are available in total.
func (s *SeekingS3) Size() (int64, error) {
	// Use cached result.
	if s.size > -1 {
		return s.size, nil
	}

	resp, err := s.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})

	if err != nil {
		return 0, err
	}

	if resp.ContentLength < 0 {
		return 0, errors.New("no content length for Size()")
	}
	if s.Debug {
		log.Printf("size %v", resp.ContentLength)
	}

	s.size = resp.ContentLength

	return resp.ContentLength, nil
}
