package internal

import (
	"context"
	"errors"
	"io"

	"golang.org/x/time/rate"
)

var DefaultBytesPerSecond = 5 * 1024 * 1024

// ThrottledReadCloser wraps an io.ReadCloser with rate limiting
type ThrottledReadCloser struct {
	reader  io.ReadCloser
	limiter *rate.Limiter
}

// Read implements io.Reader with rate limiting
func (t *ThrottledReadCloser) Read(p []byte) (n int, err error) {
	if err := t.limiter.WaitN(context.Background(), len(p)); err != nil {
		return 0, err
	}
	return t.reader.Read(p)
}

// Close implements io.Closer
func (t *ThrottledReadCloser) Close() error {
	return t.reader.Close()
}

// NewThrottledReadCloser creates a new rate-limited ReadCloser
// bytesPerSecond specifies the maximum bytes to read per second
func NewThrottledReadCloser(reader io.ReadCloser, bytesPerSecond int) io.ReadCloser {
	return &ThrottledReadCloser{
		reader:  reader,
		limiter: rate.NewLimiter(rate.Limit(bytesPerSecond), bytesPerSecond),
	}
}

// ThrottledCopyN performs a rate-limited copy of n bytes
func ThrottledCopyN(dst io.Writer, src io.Reader, n int64, bytesPerSecond int) (written int64, err error) {
	limiter := rate.NewLimiter(rate.Limit(bytesPerSecond), bytesPerSecond)

	buf := make([]byte, 32*1024) // 32KB buffer
	for written < n {
		toRead := n - written
		if toRead > int64(len(buf)) {
			toRead = int64(len(buf))
		}

		// Wait for rate limit
		if err := limiter.WaitN(context.Background(), int(toRead)); err != nil {
			return written, err
		}

		nr, er := src.Read(buf[:toRead])
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
