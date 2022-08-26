// Package s3readerat implements io.ReaderAt using S3 GetObject and Range.
package s3readerat

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3ReaderAt is io.ReaderAt implementation that makes HTTP Range Requests.
// New instances must be created with the New() function.
// It is safe for concurrent use.
type S3ReaderAt struct {
	Debug  bool
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	size   int64
}

var _ io.ReaderAt = (*S3ReaderAt)(nil)

// New creates a new S3ReaderAt.
func New(client *s3.Client, bucket string, key string) (ra *S3ReaderAt, err error) {
	if client == nil {
		return nil, errors.New("S3 client is required")
	}

	ra = &S3ReaderAt{
		ctx:    context.Background(),
		client: client,
		bucket: bucket,
		key:    key,
	}

	ra.size = -1
	return ra, nil
}

// NewWithSize creates a new S3ReaderAt that skips checking the S3 object's size.
func NewWithSize(client *s3.Client, bucket string, key string, size int64) (ra *S3ReaderAt, err error) {
	if client == nil {
		return nil, errors.New("S3 client is required")
	}

	if size < 0 {
		return nil, errors.Errorf("Provided size is invalid: %d", size)
	}

	ra = &S3ReaderAt{
		ctx:    context.Background(),
		client: client,
		bucket: bucket,
		key:    key,
	}

	ra.size = size
	return ra, nil
}

func (ra *S3ReaderAt) WithContext(ctx context.Context) *S3ReaderAt {
	ra.ctx = ctx
	return ra
}

func (ra *S3ReaderAt) Size() (int64, error) {
	if ra.size >= 0 {
		return ra.size, nil
	}

	if ra.Debug {
		log.Printf("Issuing a HeadObject request for S3 object s3://%s/%s", ra.bucket, ra.key)
	}

	resp, err := ra.client.HeadObject(ra.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(ra.bucket),
		Key:    aws.String(ra.key),
	})
	if err != nil {
		return -1, errors.Wrap(err, "S3 HeadObject failed")
	}

	if resp.ContentLength < 0 {
		return -1, errors.Errorf("S3 object size is invalid: %d", resp.ContentLength)
	}

	ra.size = resp.ContentLength
	if ra.Debug {
		log.Printf("S3 object s3://%s/%s has size %d", ra.bucket, ra.key, ra.size)
	}

	return ra.size, nil
}

// ReadAt reads len(b) bytes from the remote file starting at byte offset
// off. It returns the number of bytes read and the error, if any. ReadAt
// always returns a non-nil error when n < len(b). At end of file, that
// error is io.EOF. It is safe for concurrent use.
func (ra *S3ReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	// fmt.Printf("readat off=%d len=%d\n", off, len(p))
	if len(p) == 0 {
		return 0, nil
	}

	reqFirst := off
	reqLast := off + int64(len(p)) - 1

	_, err = ra.Size()
	if err != nil {
		return 0, err
	}

	var returnErr error
	if ra.size != -1 && reqLast > ra.size-1 {
		// Clamp down the requested range.
		reqLast = ra.size - 1
		returnErr = io.EOF

		if reqLast < reqFirst {
			return 0, io.EOF
		}

		p = p[:reqLast-reqFirst+1]
	}

	rng := fmt.Sprintf("bytes=%d-%d", reqFirst, reqLast)

	if ra.Debug {
		log.Printf("Issuing a GetObject request for S3 object s3://%s/%s with range %s", ra.bucket, ra.key, rng)
	}

	resp, err := ra.client.GetObject(ra.ctx, &s3.GetObjectInput{
		Bucket: aws.String(ra.bucket),
		Key:    aws.String(ra.key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return 0, errors.Wrap(err, "S3 GetObject error")
	}
	defer resp.Body.Close()

	n, err = io.ReadFull(resp.Body, p)

	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	if (err == nil || err == io.EOF) && int64(n) != resp.ContentLength {
		if ra.Debug {
			log.Printf("We read %d bytes, but the content-length was %d\n", n, resp.ContentLength)
		}
	}

	if err == nil && returnErr != nil {
		err = returnErr
	}

	return n, err
}
