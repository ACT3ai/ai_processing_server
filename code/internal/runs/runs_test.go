package runs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadLinesSkipsATornLastLine(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, EventsFile), []byte(`{"type":"stage","state":"RUNNING"}`+"\n"+`{"type":"progress","progress":0.5}`+"\n"+`{"type":"prog`), 0o600)
	lines, n, err := ReadLines(dir, 0)
	if err != nil || n != 2 || len(lines) != 2 || lines[1].Progress != 0.5 {
		t.Fatalf("lines %+v n %d err %v", lines, n, err)
	}
	more, n2, _ := ReadLines(dir, 2)
	if len(more) != 0 || n2 != 2 {
		t.Fatalf("re-read gave %d new lines", len(more))
	}
}

func TestAliveNeedsAFreshHeartbeatFromTheSamePid(t *testing.T) {
	dir := t.TempDir()
	rec := &Record{TaskHost: HostInfo{PID: os.Getpid()}, ClaimedAt: time.Now().Add(-time.Hour)}
	if Alive(dir, rec) {
		t.Fatal("no heartbeat long after launch must read as gone")
	}
	WriteJSON(dir, HeartbeatFile, Heartbeat{PID: os.Getpid(), At: time.Now()})
	if !Alive(dir, rec) {
		t.Fatal("a fresh heartbeat from a live pid must read as alive")
	}
	WriteJSON(dir, HeartbeatFile, Heartbeat{PID: os.Getpid() + 100000, At: time.Now()})
	if Alive(dir, rec) {
		t.Fatal("a heartbeat from another pid (pid reuse) must read as gone")
	}
	WriteJSON(dir, HeartbeatFile, Heartbeat{PID: os.Getpid(), At: time.Now().Add(-time.Minute)})
	if Alive(dir, rec) {
		t.Fatal("a stale heartbeat must read as gone")
	}
}
