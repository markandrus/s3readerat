package s3readerat

import (
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/pkg/errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

// TestNewSingleRegionSize tests that, using a single-region S3ReaderAt to access an S3 bucket in another region fails
// with a 3xx error response when calling Size.
func TestNewSingleRegionSize(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		t.Fatalf("Error loading .env file: %v", err)
	}

	s3Client := s3.New(s3.Options{
		Region: os.Getenv("AWS_REGION"),
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		),
	})

	s3ReaderAt, err := New(s3Client, os.Getenv("AWS_S3_BUCKET"), os.Getenv("AWS_S3_KEY"))
	if err != nil {
		t.Fatalf("Error calling New: %v", err)
	}

	_, err = s3ReaderAt.Size()
	if err == nil {
		t.Fatalf("Expected an error calling Size")
	}

	var responseError *awshttp.ResponseError
	if !errors.As(err, &responseError) {
		t.Fatalf("Expected a ResponseError")
	}

	if responseError.Response.StatusCode/100 != 3 {
		t.Fatalf("Expected a 3xx response")
	}
}

// TestNewSingleRegionReadAt tests that, using a single-region S3ReaderAt to access an S3 bucket in another region fails
// with a 3xx error response when calling ReadAt.
func TestNewSingleRegionReadAt(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		t.Fatalf("Error loading .env file: %v", err)
	}

	s3Client := s3.New(s3.Options{
		Region: os.Getenv("AWS_REGION"),
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		),
	})

	s3ReaderAt, err := NewWithSize(s3Client, os.Getenv("AWS_S3_BUCKET"), os.Getenv("AWS_S3_KEY"), 8)
	if err != nil {
		t.Fatalf("Error calling New: %v", err)
	}

	b := make([]byte, 8)
	_, err = s3ReaderAt.ReadAt(b, 0)
	if err == nil {
		t.Fatalf("Expected an error calling ReadAt")
	}

	var responseError *awshttp.ResponseError
	if !errors.As(err, &responseError) {
		t.Fatalf("Expected a ResponseError")
	}

	if responseError.Response.StatusCode/100 != 3 {
		t.Fatalf("Expected a 3xx response")
	}
}

// TestNewMultiRegion tests that, using a multi-region S3ReaderAt to access an S3 bucket in another region succeeds for
// both Size and ReadAt.
func TestNewMultiRegion(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		t.Fatalf("Error loading .env file: %v", err)
	}

	s3Options := s3.Options{
		Region: os.Getenv("AWS_REGION"),
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		),
	}

	s3ReaderAt, err := NewWithOptions(Options{
		Options: &s3Options,
		Bucket:  os.Getenv("AWS_S3_BUCKET"),
		Key:     os.Getenv("AWS_S3_KEY"),
	})
	if err != nil {
		t.Fatalf("Error calling New: %v", err)
	}

	if _, err = s3ReaderAt.Size(); err != nil {
		t.Fatalf("Error calling Size: %v", err)
	}

	b := make([]byte, 8)
	if _, err = s3ReaderAt.ReadAt(b, 0); err != nil {
		t.Fatalf("Error calling ReadAt: %v", err)
	}
}
