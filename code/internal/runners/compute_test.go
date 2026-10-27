package runners

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

type nopRep struct{ last float64 }

func (r *nopRep) Stage(string)                 { r.last = 0 }
func (r *nopRep) Progress(f float64, _ string) { r.last = f }
func (r *nopRep) Log(string)                   {}

func TestPrimesKnownValues(t *testing.T) {
	for _, c := range []struct {
		limit, count, sum, largest int64
		segs                       int
	}{{10, 4, 17, 7, 1}, {100, 25, 1060, 97, 7}, {1_000_000, 78498, 37550402023, 999983, 13}} {
		got, err := primes(context.Background(), c.limit, c.segs, 0, &nopRep{})
		if err != nil {
			t.Fatal(err)
		}
		r := got.(primesResult)
		if r.Count != c.count || r.Sum != c.sum || r.Largest != c.largest {
			t.Errorf("limit %d: got %+v", c.limit, r)
		}
	}
}

func TestSHA256Chain(t *testing.T) {
	h := sha256.Sum256([]byte("act3"))
	h = sha256.Sum256(h[:])
	got, _ := sha256Chain(context.Background(), "act3", 2, 0, &nopRep{})
	if got.(map[string]any)["digest"] != hex.EncodeToString(h[:]) {
		t.Fatalf("chain of 2 wrong: %v", got)
	}
}

func TestMatrixIdentityLike(t *testing.T) {
	got, err := matrix(context.Background(), 1, 7, 0, &nopRep{})
	if err != nil {
		t.Fatal(err)
	}
	g := &lcg{x: 7}
	a, b := g.next(), g.next()
	m := got.(map[string]any)
	if m["trace"].(int64) != a*b || m["sum_of_squares"].(int64) != a*b*a*b {
		t.Fatalf("1×1 product wrong: %v (a=%d b=%d)", m, a, b)
	}
}

func TestCancelStopsCompute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := primes(ctx, 1_000_000, 10, 0, &nopRep{}); err == nil {
		t.Fatal("cancelled context must stop the sieve")
	}
}

func TestMandelbrotOrigin(t *testing.T) {
	// The centre pixel of a frame centred on 0+0i is inside the set: max_iter iterations.
	_, st := mandelbrotFrame(16, 16, 50, 0, 0, 3.0)
	if st.Inside == 0 || st.IterSum <= 0 {
		t.Fatalf("expected inside pixels: %+v", st)
	}
}
