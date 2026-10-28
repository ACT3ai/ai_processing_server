package runners

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// Compute is the CPU runner behind sys.compute and sys.self_test. Every op produces a result that can be checked
// independently (the test suite recomputes each one), and reports progress in steps so the API shows it moving.
//
// params.op:
//
//	primes        {limit, segments, throttle_ms}         count, sum and largest prime ≤ limit (segmented sieve)
//	sha256_chain  {seed, iterations, throttle_ms}        sha256 applied `iterations` times to the seed
//	matrix        {n, seed, throttle_ms}                 integer matrix product C = A·B, trace/sum/sum of squares
//	file_stats    {input, throttle_ms}                   lines, words, bytes, sha256, top words of a staged input
//	sleep         {ms, steps}                            waits, reporting progress (scheduling tests)
//	flaky         {fail_before_attempt, kind}            fails until the given attempt (retry tests)
//	fail          {kind, message}                        always fails with that error kind
//	panic         {}                                     panics (panic recovery test)
type Compute struct{}

type computeParams struct {
	Op                string `json:"op"`
	Limit             int64  `json:"limit"`
	Segments          int    `json:"segments"`
	Seed              string `json:"seed"`
	Iterations        int    `json:"iterations"`
	N                 int    `json:"n"`
	MatrixSeed        uint32 `json:"matrix_seed"`
	Input             string `json:"input"`
	Ms                int    `json:"ms"`
	Steps             int    `json:"steps"`
	ThrottleMs        int    `json:"throttle_ms"`
	FailBeforeAttempt int    `json:"fail_before_attempt"`
	Kind              string `json:"kind"`
	Message           string `json:"message"`
}

func (Compute) Name() string { return "compute" }

func (Compute) Check(_ context.Context, job *jobs.Job, _ Env) error {
	var p computeParams
	if err := decodeParams(job, &p); err != nil {
		return err
	}
	switch p.Op {
	case "primes":
		if p.Limit < 2 || p.Limit > 2_000_000_000 {
			return jobs.Errf(jobs.KindInvalidInput, "primes: limit must be within 2..2000000000")
		}
	case "sha256_chain":
		if p.Iterations < 1 || p.Iterations > 100_000_000 {
			return jobs.Errf(jobs.KindInvalidInput, "sha256_chain: iterations must be within 1..100000000")
		}
	case "matrix":
		if p.N < 1 || p.N > 1500 {
			return jobs.Errf(jobs.KindInvalidInput, "matrix: n must be within 1..1500")
		}
	case "file_stats":
		if len(job.Envelope.Inputs) == 0 {
			return jobs.Errf(jobs.KindInvalidInput, "file_stats: needs one input")
		}
	case "sleep", "flaky", "fail", "panic":
	case "":
		return jobs.Errf(jobs.KindInvalidInput, "params.op is required (primes, sha256_chain, matrix, file_stats, sleep, flaky, fail, panic)")
	default:
		return jobs.Errf(jobs.KindInvalidInput, "unknown op %q", p.Op)
	}
	return nil
}

func (c Compute) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p computeParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	rep.Stage("compute:" + p.Op)
	throttle := time.Duration(p.ThrottleMs) * time.Millisecond
	var data any
	var err error
	switch p.Op {
	case "primes":
		data, err = primes(ctx, p.Limit, max(p.Segments, 1), throttle, rep)
	case "sha256_chain":
		data, err = sha256Chain(ctx, p.Seed, p.Iterations, throttle, rep)
	case "matrix":
		data, err = matrix(ctx, p.N, p.MatrixSeed, throttle, rep)
	case "file_stats":
		name := p.Input
		if name == "" {
			name = job.Envelope.Inputs[0].Name
		}
		data, err = fileStats(ctx, env.Inputs[name], throttle, rep)
	case "sleep":
		data, err = sleep(ctx, p.Ms, max(p.Steps, 1), rep)
	case "flaky":
		attempt := job.Attempt
		if attempt < p.FailBeforeAttempt {
			return nil, jobs.Errf(kindOr(p.Kind, jobs.KindTransient), "flaky: attempt %d fails on purpose (succeeds from attempt %d)", attempt, p.FailBeforeAttempt)
		}
		data = map[string]any{"op": "flaky", "succeeded_on_attempt": attempt}
	case "fail":
		return nil, jobs.Errf(kindOr(p.Kind, jobs.KindInvalidInput), "%s", strings.TrimSpace(p.Message+" (fail op)"))
	case "panic":
		panic("compute: panic op requested")
	}
	if err != nil {
		return nil, err
	}
	rep.Progress(1, "computed")
	out, err := writeJSONOutput(env, "result", data)
	if err != nil {
		return nil, err
	}
	return &Result{Outputs: []jobs.Output{out}, Data: data}, nil
}

func kindOr(k string, def jobs.ErrorKind) jobs.ErrorKind {
	if k == "" {
		return def
	}
	return jobs.ErrorKind(k)
}

// step checks cancellation, reports progress and applies the throttle between units of work.
func step(ctx context.Context, rep Reporter, frac float64, note string, throttle time.Duration) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	rep.Progress(frac, note)
	if throttle > 0 {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(throttle):
		}
	}
	return nil
}

type primesResult struct {
	Op       string `json:"op"`
	Limit    int64  `json:"limit"`
	Count    int64  `json:"count"`
	Sum      int64  `json:"sum"`
	Largest  int64  `json:"largest"`
	Segments int    `json:"segments"`
}

func primes(ctx context.Context, limit int64, segments int, throttle time.Duration, rep Reporter) (any, error) {
	root := int64(math.Sqrt(float64(limit)))
	for root*root > limit {
		root--
	}
	for (root+1)*(root+1) <= limit {
		root++
	}
	small := make([]bool, root+1) // true = composite
	var base []int64
	for i := int64(2); i <= root; i++ {
		if !small[i] {
			base = append(base, i)
			for j := i * i; j <= root; j += i {
				small[j] = true
			}
		}
	}
	res := primesResult{Op: "primes", Limit: limit, Segments: segments}
	span := (limit-1)/int64(segments) + 1
	seg := make([]bool, span)
	for s := 0; s < segments; s++ {
		lo := 2 + int64(s)*span
		hi := min(lo+span-1, limit)
		if lo > hi {
			break
		}
		clear(seg)
		for _, p := range base {
			start := max(p*p, (lo+p-1)/p*p)
			for j := start; j <= hi; j += p {
				seg[j-lo] = true
			}
		}
		for v := lo; v <= hi; v++ {
			if !seg[v-lo] {
				res.Count++
				res.Sum += v
				res.Largest = v
			}
		}
		if err := step(ctx, rep, float64(s+1)/float64(segments), fmt.Sprintf("segment %d/%d up to %d: %d primes so far", s+1, segments, hi, res.Count), throttle); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func sha256Chain(ctx context.Context, seed string, iterations int, throttle time.Duration, rep Reporter) (any, error) {
	h := sha256.Sum256([]byte(seed))
	steps := 20
	every := max(iterations/steps, 1)
	for i := 1; i < iterations; i++ {
		h = sha256.Sum256(h[:])
		if (i+1)%every == 0 {
			if err := step(ctx, rep, float64(i+1)/float64(iterations), fmt.Sprintf("%d/%d hashes", i+1, iterations), throttle); err != nil {
				return nil, err
			}
		}
	}
	return map[string]any{"op": "sha256_chain", "seed": seed, "iterations": iterations, "digest": hex.EncodeToString(h[:])}, nil
}

// lcg is the generator the test suite reimplements: x = (1103515245·x + 12345) mod 2^31, value = x mod 101 − 50.
type lcg struct{ x uint32 }

func (l *lcg) next() int64 {
	l.x = (1103515245*l.x + 12345) & 0x7fffffff
	return int64(l.x%101) - 50
}

func matrix(ctx context.Context, n int, seed uint32, throttle time.Duration, rep Reporter) (any, error) {
	g := &lcg{x: seed}
	a := make([][]int64, n)
	b := make([][]int64, n)
	for i := range n {
		a[i] = make([]int64, n)
		for j := range n {
			a[i][j] = g.next()
		}
	}
	for i := range n {
		b[i] = make([]int64, n)
		for j := range n {
			b[i][j] = g.next()
		}
	}
	var trace, sum, sumSq int64
	every := max(n/20, 1)
	for i := range n {
		for j := range n {
			var acc int64
			for k := range n {
				acc += a[i][k] * b[k][j]
			}
			sum += acc
			sumSq += acc * acc
			if i == j {
				trace += acc
			}
		}
		if (i+1)%every == 0 || i == n-1 {
			if err := step(ctx, rep, float64(i+1)/float64(n), fmt.Sprintf("row %d/%d", i+1, n), throttle); err != nil {
				return nil, err
			}
		}
	}
	return map[string]any{"op": "matrix", "n": n, "matrix_seed": seed, "trace": trace, "sum": sum, "sum_of_squares": sumSq}, nil
}

func fileStats(ctx context.Context, path string, throttle time.Duration, rep Reporter) (any, error) {
	if path == "" {
		return nil, jobs.Errf(jobs.KindInvalidInput, "file_stats: input was not staged")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "open staged input")
	}
	defer f.Close()
	st, _ := f.Stat()
	total := max(st.Size(), 1)
	h := sha256.New()
	r := bufio.NewReaderSize(f, 1<<16)
	var lines, words, bytesRead int64
	counts := map[string]int64{}
	inWord := false
	var cur strings.Builder
	buf := make([]byte, 1<<16)
	for {
		n, err := r.Read(buf)
		chunk := buf[:n]
		h.Write(chunk)
		bytesRead += int64(n)
		for _, c := range chunk {
			if c == '\n' {
				lines++
			}
			if c < 128 && unicode.IsSpace(rune(c)) {
				if inWord {
					words++
					counts[strings.ToLower(cur.String())]++
					cur.Reset()
				}
				inWord = false
			} else {
				inWord = true
				cur.WriteByte(c)
			}
		}
		if n > 0 {
			if e := step(ctx, rep, float64(bytesRead)/float64(total), fmt.Sprintf("%d/%d bytes", bytesRead, total), throttle); e != nil {
				return nil, e
			}
		}
		if err != nil {
			break
		}
	}
	if inWord {
		words++
		counts[strings.ToLower(cur.String())]++
	}
	type wc struct {
		Word  string `json:"word"`
		Count int64  `json:"count"`
	}
	var top []wc
	for w, c := range counts {
		top = append(top, wc{w, c})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].Count != top[j].Count {
			return top[i].Count > top[j].Count
		}
		return top[i].Word < top[j].Word
	})
	if len(top) > 5 {
		top = top[:5]
	}
	return map[string]any{"op": "file_stats", "lines": lines, "words": words, "bytes": bytesRead,
		"sha256": hex.EncodeToString(h.Sum(nil)), "top_words": top}, nil
}

func sleep(ctx context.Context, ms, steps int, rep Reporter) (any, error) {
	per := time.Duration(ms) * time.Millisecond / time.Duration(steps)
	for s := 1; s <= steps; s++ {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(per):
		}
		rep.Progress(float64(s)/float64(steps), fmt.Sprintf("step %d/%d", s, steps))
	}
	return map[string]any{"op": "sleep", "ms": ms, "steps": steps}, nil
}
