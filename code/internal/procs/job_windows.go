package procs

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows Job Objects (pm/cancel.mdx §5.4): Windows has no process groups, and `taskkill /T` follows parentage, which
// a child that outlived its parent no longer has. Each step therefore runs in a NAMED job object with
// KILL_ON_JOB_CLOSE: every process the step starts is in it, the controller can list and terminate the whole job by
// name, and when the last handle closes (the task host exits or dies) every process still in it is killed.

var (
	kernel32         = windows.NewLazySystemDLL("kernel32.dll")
	procOpenJobObjec = kernel32.NewProc("OpenJobObjectW")
)

const (
	jobObjectQuery     = 0x0004
	jobObjectTerminate = 0x0008
)

type jobHandle struct{ h windows.Handle }

func jobName(name string) (*uint16, error) { return windows.UTF16PtrFromString(`Local\` + name) }

// attachJob puts the started process into a new named job object. A child the process starts in its first
// milliseconds, before this call, would escape it; Python and ffmpeg start no child that early.
func attachJob(pid int, name string) *jobHandle {
	if name == "" {
		return nil
	}
	n, err := jobName(name)
	if err != nil {
		return nil
	}
	job, err := windows.CreateJobObject(nil, n)
	if err != nil {
		elog.Warn("creating the step's job object", err, "job", name)
		return nil
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		elog.Warn("setting kill-on-close on the step's job object", err, "job", name)
	}
	p, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		windows.CloseHandle(job)
		return nil
	}
	defer windows.CloseHandle(p)
	if err := windows.AssignProcessToJobObject(job, p); err != nil {
		elog.Warn("assigning the step to its job object", err, "job", name)
		windows.CloseHandle(job)
		return nil
	}
	return &jobHandle{h: job}
}

func (j *jobHandle) terminate() {
	if j != nil {
		_ = windows.TerminateJobObject(j.h, 1)
	}
}

// release closes our handle: with KILL_ON_JOB_CLOSE, whatever the step left running dies now.
func (j *jobHandle) release() {
	if j != nil {
		windows.CloseHandle(j.h)
	}
}

func openJob(name string) (windows.Handle, bool) {
	n, err := jobName(name)
	if err != nil {
		return 0, false
	}
	r, _, _ := procOpenJobObjec.Call(uintptr(jobObjectQuery|jobObjectTerminate), 0, uintptr(unsafe.Pointer(n)))
	if r == 0 {
		return 0, false
	}
	return windows.Handle(r), true
}

// JobPIDs lists the live processes in the named job; exists=false when no such job is open anywhere (every process in
// it is gone: kill-on-close).
func JobPIDs(name string) (pids []int, exists bool) {
	h, ok := openJob(name)
	if !ok {
		return nil, false
	}
	defer windows.CloseHandle(h)
	type list struct {
		Assigned, InList uint32
		IDs              [1024]uintptr
	}
	var l list
	if err := windows.QueryInformationJobObject(h, windows.JobObjectBasicProcessIdList, uintptr(unsafe.Pointer(&l)),
		uint32(unsafe.Sizeof(l)), nil); err != nil && err != syscall.Errno(234) { // 234: ERROR_MORE_DATA
		return nil, true
	}
	for i := uint32(0); i < l.InList && i < uint32(len(l.IDs)); i++ {
		pids = append(pids, int(l.IDs[i]))
	}
	return pids, true
}

// KillJob terminates every process in the named job.
func KillJob(name string) {
	if h, ok := openJob(name); ok {
		_ = windows.TerminateJobObject(h, 1)
		windows.CloseHandle(h)
	}
}
