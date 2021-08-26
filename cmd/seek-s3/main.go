package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	seekings3 "mrkrbrts.com/seekings3"
)

var debug = flag.Bool("debug", false, "enable verbose output")
var offset = flag.Int64("offset", -8, "offset parameter to seek")
var whence = flag.Int("whence", 2, "whence parameter to seek (0 is start, 1 is current and 2 is end)")
var limit = flag.Int64("limit", -1, "limit the bytes to print (-1 is unlimited)")

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		log.Fatal("Expected an S3 URL")
	}

	parsed, err := url.Parse(flag.Arg(0))
	if err != nil {
		log.Fatalf("Failed to parse S3 URL: %v", err)
	}

	if *whence < 0 || *whence > 2 {
		log.Fatal("Whence parameter must be 0, 1 or 2")
	}

	if *limit < -1 || *limit == 0 {
		log.Fatal("Limit parameter must be -1 or positive")
	}

	bucket := parsed.Host
	key := strings.TrimPrefix(parsed.Path, "/")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	reader := seekings3.New(client, bucket, key)
	reader.Debug = *debug

	_, err = reader.Seek(*offset, *whence)
	if err != nil {
		log.Fatalf("Failed to seek to (offset=%d, whence=%d): %v", *offset, *whence, err)
	}

	var bytes int64
	if *limit == -1 {
		bytes, err = io.Copy(os.Stdout, reader)
	} else {
		bytes, err = io.CopyN(os.Stdout, reader, *limit)
	}

	if err != nil && err != io.EOF {
		log.Fatalf("Failed to read S3 object: %v", err)
	}

	log.Printf("Read %d bytes from S3 object\n", bytes)
}
