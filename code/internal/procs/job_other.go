//go:build !windows

package procs

// Job objects exist only on Windows; process groups do the same work elsewhere.
type jobHandle struct{}

func attachJob(int, string) *jobHandle { return nil }
func (*jobHandle) terminate()          {}
func (*jobHandle) release()            {}
func JobPIDs(string) ([]int, bool)     { return nil, false }
func KillJob(string)                   {}
