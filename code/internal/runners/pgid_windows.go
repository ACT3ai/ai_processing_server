package runners

// pgidOf: Windows has no process groups; holdsGPU matches the step processes' own pids instead.
func pgidOf(int) (int, bool) { return 0, false }
