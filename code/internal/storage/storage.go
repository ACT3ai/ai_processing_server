// Package storage stages job inputs and uploads outputs: streaming from disk, sha256 on the way, and retries
// for transient failures inside the stage only (spec §9.7, §12.1).
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/logging"
)

// Backoff is the wait before each retry of a transient stage failure (spec §12.2: 1 s, 4 s, 16 s).
var Backoff = []time.Duration{1 * time.Second, 4 * time.Second, 16 * time.Second}

var client = &http.Client{Timeout: 0} // stage timeouts come from the job context

// Fetch stages one input into dir and returns its local path, size and sha256.
// Local paths are accepted only when allowLocal (direct mode).
func Fetch(ctx context.Context, in jobs.Input, dir string, allowLocal bool) (string, int64, string, error) {
	if in.Name == "" || strings.ContainsAny(in.Name, `/\`) || in.Name == "." || in.Name == ".." {
		return "", 0, "", jobs.Errf(jobs.KindInvalidInput, "input name %q must be a plain file name", in.Name)
	}
	dst := filepath.Join(dir, in.Name)
	var size int64
	var sum string
	var err error
	switch {
	case in.Path != "":
		if !allowLocal {
			return "", 0, "", jobs.Errf(jobs.KindInvalidInput, "input %s: local paths are only allowed in direct mode", in.Name)
		}
		size, sum, err = copyFile(in.Path, dst)
		if err != nil {
			return "", 0, "", jobs.Wrap(jobs.KindInvalidInput, err, "input "+in.Name+" could not be read")
		}
	case strings.HasPrefix(in.URL, "http://"), strings.HasPrefix(in.URL, "https://"):
		err = withRetry(ctx, func() error {
			var e error
			size, sum, e = download(ctx, in.URL, dst)
			return e
		})
		if err != nil {
			return "", 0, "", err
		}
	default:
		return "", 0, "", jobs.Errf(jobs.KindInvalidInput, "input %s needs an http(s) url or a path", in.Name)
	}
	if in.SHA256 != "" && !strings.EqualFold(in.SHA256, sum) {
		return "", 0, "", jobs.Errf(jobs.KindInvalidInput, "input %s sha256 mismatch: expected %s, got %s", in.Name, in.SHA256, sum)
	}
	if in.Bytes > 0 && in.Bytes != size {
		return "", 0, "", jobs.Errf(jobs.KindInvalidInput, "input %s size mismatch: expected %d bytes, got %d", in.Name, in.Bytes, size)
	}
	return dst, size, sum, nil
}

func download(ctx context.Context, url, dst string) (int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", jobs.Wrap(jobs.KindInvalidInput, err, "bad input url")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", jobs.Wrap(jobs.KindTransient, err, "download "+logging.RedactURL(url)+" failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return 0, "", jobs.Errf(jobs.KindTransient, "download %s: HTTP %d", logging.RedactURL(url), resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", jobs.Errf(jobs.KindInvalidInput, "download %s: HTTP %d", logging.RedactURL(url), resp.StatusCode)
	}
	f, err := os.Create(dst)
	if err != nil {
		return 0, "", jobs.Wrap(jobs.KindInfra, err, "cannot write input to the work dir")
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), resp.Body)
	if err != nil {
		return 0, "", jobs.Wrap(jobs.KindTransient, err, "download "+logging.RedactURL(url)+" interrupted")
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// Put uploads a file with the given method (PUT by default), streaming with Content-Length.
func Put(ctx context.Context, path string, up jobs.Upload, contentType string) error {
	method := up.Method
	if method == "" {
		method = http.MethodPut
	}
	return withRetry(ctx, func() error {
		f, err := os.Open(path)
		if err != nil {
			return jobs.Wrap(jobs.KindInternal, err, "output file missing")
		}
		defer f.Close()
		st, _ := f.Stat()
		req, err := http.NewRequestWithContext(ctx, method, up.URL, f)
		if err != nil {
			return jobs.Wrap(jobs.KindInvalidInput, err, "bad upload url")
		}
		req.ContentLength = st.Size()
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := client.Do(req)
		if err != nil {
			return jobs.Wrap(jobs.KindTransient, err, "upload to "+logging.RedactURL(up.URL)+" failed")
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return jobs.Errf(jobs.KindTransient, "upload to %s: HTTP %d", logging.RedactURL(up.URL), resp.StatusCode)
		}
		if resp.StatusCode >= 300 {
			return jobs.Errf(jobs.KindInfra, "upload to %s rejected: HTTP %d", logging.RedactURL(up.URL), resp.StatusCode)
		}
		return nil
	})
}

// HashFile returns size and sha256 of a file.
func HashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// MoveFile renames, falling back to copy across filesystems.
func MoveFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if _, _, err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src, dst string) (int64, string, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, "", err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return 0, "", err
	}
	defer out.Close()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), in)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// withRetry retries fn on transient errors with Backoff, stopping early if ctx ends.
func withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		var je *jobs.Error
		if err == nil || !errors.As(err, &je) || je.Kind != jobs.KindTransient || attempt >= len(Backoff) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up retrying: %v)", err, context.Cause(ctx))
		case <-time.After(Backoff[attempt]):
		}
	}
}
