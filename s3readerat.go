// Package s3readerat implements io.ReaderAt using S3 GetObject and Range.
package s3readerat

import (
	"context"
	"fmt"
	"io"
	"log"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3ReaderAt is io.ReaderAt implementation that makes HTTP Range Requests.
// New instances must be created with the New() function.
// It is safe for concurrent use.
type S3ReaderAt struct {
	Debug   bool
	ctx     context.Context
	client  *s3.Client
	options *s3.Options
	bucket  string
	key     string
	size    int64
}

type Options struct {
	// Debug indicates whether to enable debug logging.
	Debug bool

	// Context is the context.Context to use.
	Context context.Context

	// Client is the s3.Client to use when running in single-region mode. You can instead pass s3.Options to run in
	// multi-region mode.
	Client *s3.Client

	// Options are the s3.Options to use when running in multi-region mode. In this mode, S3ReaderAt constructs the
	// s3.Client for you in the appropriate region(s). You can instead pass s3.Client to run in single-region mode.
	Options *s3.Options

	// Bucket is the AWS S3 bucket to use.
	Bucket string

	// Key is the key to use within the AWS S3 bucket. It should not start with a leading slash.
	Key string

	// Size is the size in bytes to use, if known in advance. This is an optimization that avoids calling "HeadObject".
	Size *int64
}

var _ io.ReaderAt = (*S3ReaderAt)(nil)

// New creates a new S3ReaderAt.
func New(client *s3.Client, bucket string, key string) (*S3ReaderAt, error) {
	return NewWithOptions(Options{
		Context: context.Background(),
		Client:  client,
		Bucket:  bucket,
		Key:     key,
	})
}

// NewWithSize creates a new S3ReaderAt that skips checking the S3 object's size.
func NewWithSize(client *s3.Client, bucket string, key string, size int64) (*S3ReaderAt, error) {
	return NewWithOptions(Options{
		Context: context.Background(),
		Client:  client,
		Bucket:  bucket,
		Key:     key,
		Size:    &size,
	})
}

func NewWithOptions(options Options) (*S3ReaderAt, error) {
	if options.Client == nil && options.Options == nil {
		return nil, errors.New("one of Client or Options is required")
	} else if options.Client != nil && options.Options != nil {
		return nil, errors.New("only one of Client or Options can be provided")
	} else if options.Size != nil && *options.Size < 0 {
		return nil, errors.Errorf("provided size is invalid: %d", *options.Size)
	}

	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	
	ra := &S3ReaderAt{
		Debug:   options.Debug,
		ctx:     ctx,
		client:  options.Client,
		options: options.Options,
		bucket:  options.Bucket,
		key:     options.Key,
	}
	
	if options.Size != nil {
		ra.size = *options.Size
	} else {
		ra.size = -1
	}
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

	resp, err := ra.headObject(ra.ctx, &s3.HeadObjectInput{
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
func (ra *S3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	// fmt.Printf("readat off=%d len=%d\n", off, len(p))
	if len(p) == 0 {
		return 0, nil
	}

	reqFirst := off
	reqLast := off + int64(len(p)) - 1

	_, err := ra.Size()
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

	resp, err := ra.getObject(ra.ctx, &s3.GetObjectInput{
		Bucket: aws.String(ra.bucket),
		Key:    aws.String(ra.key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return 0, errors.Wrap(err, "S3 GetObject error")
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p)

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

func (ra *S3ReaderAt) s3Client() *s3.Client {
	if ra.client != nil {
		return ra.client
	}

	if ra.options == nil {
		return nil
	}

	ra.client = s3.New(*ra.options)

	return ra.client
}

func (ra *S3ReaderAt) s3ClientInRegion(region string) *s3.Client {
	// Single-region mode.
	if ra.options == nil {
		return nil
	}

	// Multi-region mode. Already have s3.Client.
	if ra.options.Region == region && ra.client != nil {
		return ra.client
	}

	// Multi-region mode. Need a new s3.Client.
	options := (*ra.options).Copy()
	options.Region = region
	return s3.New(options)
}

func (ra *S3ReaderAt) headObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	client := ra.s3Client()

	resp, originalErr := client.HeadObject(ctx, input)
	if originalErr == nil {
		return resp, nil
	}

	region, err := extractRegionFromError(originalErr)
	if err != nil {
		return nil, err
	}

	client = ra.s3ClientInRegion(region)
	if client == nil {
		return nil, originalErr
	}

	return client.HeadObject(ctx, input)
}

func (ra *S3ReaderAt) getObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	client := ra.s3Client()

	resp, originalErr := client.GetObject(ctx, input)
	if originalErr == nil {
		return resp, nil
	}

	region, err := extractRegionFromError(originalErr)
	if err != nil {
		return nil, err
	}

	client = ra.s3ClientInRegion(region)
	if client == nil {
		return nil, originalErr
	}

	return client.GetObject(ctx, input)
}

// extractRegionFromError returns the value of the x-amz-bucket-region header included in any 3xx response from S3. If
// err is not a ResponseError representing a 3xx response, or if the x-amz-bucket-region header is missing, it returns
// the original err.
func extractRegionFromError(err error) (string, error) {
	var responseError *awshttp.ResponseError

	if errors.As(err, &responseError) {
		if responseError.Response.StatusCode/100 != 3 {
			return "", err
		}

		region := responseError.Response.Header.Get("X-Amz-Bucket-Region")
		if region == "" {
			return "", err
		}

		return region, nil
	}

	return "", err
}
