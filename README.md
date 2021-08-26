seekings3
=========

An implementation of `io.ReaderAt` that works using S3 GetObject and Range.
Inspired by [seekinghttp][seekinghttp] and [httpreadat][httpreaderat].

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

[seekinghttp]: https://github.com/jeffallen/seekinghttp
[httpreadat]: https://github.com/snabb/httpreaderat
