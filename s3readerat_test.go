package s3readerat

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

func TestNew(t *testing.T) {
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

	if _, err = s3ReaderAt.Size(); err != nil {
		t.Fatalf("Error calling Size: %v", err)
	}

	b := make([]byte, 8)
	if _, err = s3ReaderAt.ReadAt(b, 0); err != nil {
		t.Fatalf("Error calling ReadAt: %v", err)
	}
}
