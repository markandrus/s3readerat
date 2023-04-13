s3readerat
==========

An implementation of `io.ReaderAt` that works using S3 GetObject and Range.
Inspired by [seekinghttp][seekinghttp] and [httpreaderat][httpreaderat].

Example
-------

You can try out the included command-line program, `seek-s3`. It will let you
fetch ranges of an S3 object, using the `io.ReaderAt` interface provided by
seekings3.

```
$ go build ./cmd/seek-s3
$ ./seek-s3 -help
Usage of ./seek-s3:
  -debug
    	enable verbose output
  -limit int
    	limit the bytes to print (-1 is unlimited) (default -1)
  -offset int
    	offset parameter to seek (default -8)
  -whence int
    	whence parameter to seek (0 is start, 1 is current and 2 is end) (default 2)
```

For example, assuming your S3 object is a Parquet file, you can read the last 4
bytes.

```
$ ./seek-s3 -offset -4 -whence 2 -limit 4 s3://$BUCKET/$KEY | xxd
00000000: 5041 5231                                PAR1
```

### Single- and multi-region modes

If you call `NewWithOptions` passing an `s3.Client`, then the `S3ReaderAt` will
be in "single-region" mode. What that means is that, if the S3 bucket you are
trying to access is in another region, you may fail with a 3xx response from S3.

If you call `NewWithOptions` passing `s3.Options`, then the `S3ReaderAt` will be
in "multi-region" mode. What that means is that, if the S3 bucket you are trying
to access is in another region, the `S3ReaderAt` will construct an `s3.Client`
for you in the appropriate region, thereby avoiding the 3xx response from S3.

[seekinghttp]: https://github.com/jeffallen/seekinghttp
[httpreaderat]: https://github.com/snabb/httpreaderat
