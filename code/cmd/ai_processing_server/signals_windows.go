package main

import (
	"net/http"
	"os"
	"time"
)

var (
	stopSignals  = []os.Signal{os.Interrupt}
	reloadSignal os.Signal
)

// alive asks the local API, because Windows has no signal 0.
func alive(_ *os.Process) bool {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://127.0.0.1:9440/v1/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

// terminate drains over the local API, waits for running jobs, then ends the process (no SIGTERM on Windows).
func terminate(p paths, proc *os.Process) error {
	c, err := newClient(p)
	if err != nil {
		return err
	}
	if err := c.do("POST", "/v1/drain", &map[string]any{}); err != nil {
		return err
	}
	for {
		var info struct {
			Active int `json:"active_jobs"`
		}
		if err := c.do("GET", "/v1/server", &info); err != nil || info.Active == 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	return proc.Kill()
}
