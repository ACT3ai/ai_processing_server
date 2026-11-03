package main

// queue ls · queue show <id> · cancel <id>... — the task queue from the shell, for debugging a render server
// (pm/task_queue.mdx §13 local API, §16.5 cancellation). Everything goes through the running server's local API.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// apiError is a local API refusal with its status and error code (not_found, already_final, broker_missing, ...).
type apiError struct {
	Status  int
	Code    string
	Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.Code, e.Message) }

func isCode(err error, codes ...string) bool {
	var ae *apiError
	if !errors.As(err, &ae) {
		return false
	}
	for _, c := range codes {
		if ae.Code == c {
			return true
		}
	}
	return false
}

// call is do with the API's error code kept, so a caller can tell "not found" from "already finished".
func (c *client) call(method, path string, out any) error {
	req, _ := http.NewRequest(method, c.base+path, nil)
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("server not reachable at %s: %w (start it: just run)", c.base, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		_ = json.Unmarshal(b, &e)
		if e.Error.Code == "" {
			e.Error.Code, e.Error.Message = fmt.Sprintf("http_%d", resp.StatusCode), strings.TrimSpace(string(b))
		}
		return &apiError{Status: resp.StatusCode, Code: e.Error.Code, Message: e.Error.Message}
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

type queueRow struct {
	JobID         string    `json:"job_id"`
	Service       string    `json:"service"`
	CallerJobType string    `json:"caller_job_type"`
	Band          string    `json:"band"`
	Sequence      uint64    `json:"sequence"`
	AdmittedAt    time.Time `json:"admitted_at"`
	State         string    `json:"state"`
	Attempt       int       `json:"attempt"`
	Server        string    `json:"server"`
	Cancel        bool      `json:"cancel"`
	Held          bool      `json:"held"`
}

type queueJobView struct {
	State struct {
		State     string    `json:"state"`
		Caller    string    `json:"caller"`
		Service   string    `json:"service"`
		Band      string    `json:"band"`
		Attempt   int       `json:"attempt"`
		Server    string    `json:"server"`
		Kind      string    `json:"kind"`
		Message   string    `json:"message"`
		UpdatedAt time.Time `json:"updated_at"`
	} `json:"job_state"`
	Control queue.Control `json:"control"`
	Events  []struct {
		Seq      int64     `json:"seq"`
		Type     string    `json:"type"`
		State    string    `json:"state"`
		Stage    string    `json:"stage"`
		Progress float64   `json:"progress"`
		Note     string    `json:"note"`
		Message  string    `json:"message"`
		Kind     string    `json:"kind"`
		By       string    `json:"by"`
		At       time.Time `json:"at"`
	} `json:"events"`
}

type localJob struct {
	ID       string  `json:"id"`
	Type     string  `json:"type"`
	State    string  `json:"state"`
	Stage    string  `json:"stage"`
	Step     string  `json:"step"`
	Progress float64 `json:"progress"`
	Note     string  `json:"note"`
	LastErr  string  `json:"last_err"`
}

// localFinal: the local job table's Asynq-style states that end a job.
func localFinal(state string) bool {
	return state == "completed" || state == "archived" || state == "cancelled"
}

func cmdQueue(p paths, args []string) error {
	sub := "ls"
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	c, err := newClient(p)
	if err != nil {
		return err
	}
	switch sub {
	case "ls", "list":
		return queueList(c, p)
	case "show":
		if len(args) != 1 {
			return errors.New("usage: ai_processing_server queue show <job_id> [--lines N]")
		}
		return queueShow(c, p, args[0])
	}
	return errors.New("usage: ai_processing_server queue ls | queue show <job_id>")
}

// queueList prints what the WORK stream holds: jobs waiting for a slot and claims in flight.
func queueList(c *client, p paths) error {
	var out struct {
		Jobs []queueRow `json:"jobs"`
	}
	if err := c.call("GET", "/v1/queue/jobs?limit=500", &out); err != nil {
		return err
	}
	if p.JSONOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	if len(out.Jobs) == 0 {
		fmt.Println("the task queue holds no jobs (nothing waiting, nothing claimed)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSERVICE\tBAND\tSTATE\tTRY\tSERVER\tHELD HERE\tCANCEL\tWAITING SINCE")
	for _, j := range out.Jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n", j.JobID, j.Service, j.Band, orDash(j.State), j.Attempt,
			orDash(j.Server), yes(j.Held), yes(j.Cancel), since(j.AdmittedAt))
	}
	return tw.Flush()
}

// queueShow prints everything known about one job: job_state, cancel flag, event timeline, local record, files.
func queueShow(c *client, p paths, id string) error {
	var qj queueJobView
	qerr := c.call("GET", "/v1/queue/jobs/"+url.PathEscape(id), &qj)
	var lj localJob
	lerr := c.call("GET", "/v1/jobs/"+url.PathEscape(id), &lj)
	if qerr != nil && lerr != nil {
		if isCode(qerr, "not_found") && isCode(lerr, "not_found") {
			return fmt.Errorf("%s: not on the task queue and not in this server's job table", id)
		}
		return fmt.Errorf("%s: queue: %v; local: %v", id, qerr, lerr)
	}
	if p.JSONOut {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"queue": qj, "local": lj,
			"queue_error": errString(qerr), "local_error": errString(lerr)})
	}
	fmt.Println(id)
	if qerr == nil {
		st := qj.State
		fmt.Printf("  queue      %s · try %d · server %s · band %s · caller %s · %s · updated %s\n", st.State, st.Attempt,
			orDash(st.Server), st.Band, st.Caller, st.Service, clock(st.UpdatedAt))
		if st.Kind != "" || st.Message != "" {
			fmt.Printf("  reason     %s %s\n", st.Kind, st.Message)
		}
		if qj.Control.Cancel {
			fmt.Printf("  cancel     asked by %s\n", orDash(qj.Control.By))
		}
	} else {
		fmt.Printf("  queue      %v\n", qerr)
	}
	if lerr == nil {
		note := lj.Note
		if lj.LastErr != "" {
			note = lj.LastErr
		}
		fmt.Printf("  local      %s · %s · %.0f%% · %s\n", lj.State, orDash(lj.Stage), lj.Progress*100, note)
	} else if !isCode(lerr, "not_found") {
		fmt.Printf("  local      %v\n", lerr)
	}
	if qerr == nil && len(qj.Events) > 0 {
		fmt.Println("\n  EVENTS")
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		for _, e := range qj.Events {
			what := e.Stage
			if what == "" {
				what = e.State
			}
			text := e.Note
			if e.Message != "" {
				text = strings.TrimSpace(e.Kind + " " + e.Message)
			}
			if e.By != "" {
				text = strings.TrimSpace(text + " by " + e.By)
			}
			pct := ""
			if e.Progress > 0 {
				pct = fmt.Sprintf("%.0f%%", e.Progress*100)
			}
			fmt.Fprintf(tw, "  %s\t%d\t%s\t%s\t%s\t%s\n", clock(e.At), e.Seq, e.Type, what, pct, clip(text, 90))
		}
		tw.Flush()
	}
	fmt.Println("\n  FILES")
	logPath := filepath.Join(p.State, "logs", "jobs", id+".log")
	for _, f := range []struct{ label, path string }{
		{"job log", logPath},
		{"run dir", filepath.Join(p.State, "runs", id)},
		{"result", filepath.Join(p.State, "results", id)},
	} {
		mark := "(none)"
		if _, err := os.Stat(f.path); err == nil {
			mark = ""
		}
		fmt.Printf("  %-8s %s %s\n", f.label, f.path, mark)
	}
	if p.Lines > 0 {
		if b, err := os.ReadFile(logPath); err == nil {
			lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
			fmt.Printf("\n  LAST %d LOG LINES\n", min(p.Lines, len(lines)))
			for _, l := range lines[max(0, len(lines)-p.Lines):] {
				fmt.Println("  " + clip(l, 200))
			}
		}
	}
	return nil
}

// cmdCancel cancels jobs by id (or every unfinished one with --prefix) on whichever route holds them, and by default
// waits until each has really stopped. A finished job is reported, not "cancelled".
func cmdCancel(p paths, ids []string) error {
	c, err := newClient(p)
	if err != nil {
		return err
	}
	if p.Prefix != "" {
		found, err := unfinishedWithPrefix(c, p.Prefix)
		if err != nil {
			return err
		}
		if len(found) == 0 {
			fmt.Printf("no unfinished job starts with %q\n", p.Prefix)
		}
		ids = append(ids, found...)
	}
	if len(ids) == 0 && p.Prefix == "" {
		return errors.New("usage: ai_processing_server cancel [--prefix P] [--no-wait] <job_id>...")
	}
	failed := 0
	for _, id := range ids {
		if err := cancelOne(c, p, id); err != nil {
			fmt.Printf("%s: %v\n", id, err)
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d cancel(s) failed", failed, len(ids))
	}
	return nil
}

// unfinishedWithPrefix: ids on the task queue or in the local job table that start with prefix and are not final.
func unfinishedWithPrefix(c *client, prefix string) ([]string, error) {
	seen := map[string]bool{}
	var ids []string
	add := func(id string) {
		if strings.HasPrefix(id, prefix) && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	var q struct {
		Jobs []queueRow `json:"jobs"`
	}
	if err := c.call("GET", "/v1/queue/jobs?limit=1000", &q); err != nil && !isCode(err, "broker_missing") {
		return nil, err
	}
	for _, j := range q.Jobs {
		if !queue.FinalState(j.State) {
			add(j.JobID)
		}
	}
	var l struct {
		Jobs []localJob `json:"jobs"`
	}
	if err := c.call("GET", "/v1/jobs?limit=500", &l); err != nil {
		return nil, err
	}
	for _, j := range l.Jobs {
		if !localFinal(j.State) {
			add(j.ID)
		}
	}
	return ids, nil
}

func cancelOne(c *client, p paths, id string) error {
	esc := url.PathEscape(id)
	var qj queueJobView
	route := "queue"
	err := c.call("GET", "/v1/queue/jobs/"+esc, &qj)
	switch {
	case err == nil && queue.FinalState(qj.State.State):
		fmt.Printf("%s: already %s at %s — nothing to cancel\n", id, qj.State.State, clock(qj.State.UpdatedAt))
		return nil
	case err == nil:
		err = c.call("POST", "/v1/queue/jobs/"+esc+"/cancel", nil)
	case isCode(err, "not_found", "broker_missing", "broker_timeout"):
		route = "local"
		err = c.call("POST", "/v1/jobs/"+esc+"/cancel", nil)
		if isCode(err, "not_found") {
			return errors.New("not on the task queue and not in this server's job table")
		}
	}
	if isCode(err, "already_final") {
		fmt.Printf("%s: already finished — nothing to cancel (%v)\n", id, err)
		return nil
	}
	if err != nil {
		return err
	}
	if p.NoWait {
		fmt.Printf("%s: cancel asked (%s route)\n", id, route)
		return nil
	}
	t0 := time.Now()
	last := ""
	for time.Since(t0) < 90*time.Second {
		if route == "queue" {
			if c.call("GET", "/v1/queue/jobs/"+esc, &qj) == nil {
				last = qj.State.State
				if queue.FinalState(last) {
					fmt.Printf("%s: %s after %s (queue)\n", id, last, time.Since(t0).Round(100*time.Millisecond))
					return nil
				}
			}
		} else {
			var lj localJob
			if c.call("GET", "/v1/jobs/"+esc, &lj) == nil {
				last = lj.State
				if localFinal(last) {
					fmt.Printf("%s: %s after %s (local)\n", id, last, time.Since(t0).Round(100*time.Millisecond))
					return nil
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("cancel asked, but still %s after 90 s: the task host may still be stopping its processes "+
		"(see: ai_processing_server queue show %s)", orDash(last), id)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func yes(b bool) string {
	if b {
		return "yes"
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
	if len(s) > n {
		return s[:n-1] + "…"
	}
	return s
}

func clock(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("Jan 02 15:04:05")
}

func since(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return clock(t) + " (" + time.Since(t).Round(time.Second).String() + ")"
}
