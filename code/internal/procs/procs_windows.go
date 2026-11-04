package procs

import (
	"os/exec"
	"strconv"
	"syscall"
)

// createNoWindow: the task host is a detached process with no console, so a console program it starts (python,
// ffmpeg, taskkill) would otherwise open a visible window of its own.
const createNoWindow = 0x08000000

// belowNormalPriority: a render never starves the desktop or the server (its children inherit the class).
const belowNormalPriority = 0x00004000

func setGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | createNoWindow | belowNormalPriority, HideWindow: true}
}

func lowerPriority(*exec.Cmd) {} // set at creation (belowNormalPriority)

// Windows has no SIGINT for another process group; ask taskkill to end the tree politely first.
func interruptGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		taskkill("/T", "/PID", strconv.Itoa(cmd.Process.Pid))
	}
}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		taskkill("/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
	}
}

func taskkill(args ...string) {
	c := exec.Command("taskkill", args...)
	c.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNoWindow, HideWindow: true}
	_ = c.Run()
}
