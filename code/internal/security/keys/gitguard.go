package keys

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// CheckNotCommittable refuses a private key path that sits inside a git work tree without being git-ignored
// (security.mdx §4.4 rule 1). key_dir may point into another repo, such as the act3ai-credentials checkout, so the
// guard asks that repo's own git: a key there is fine only under an ignored path (that repo ignores /keys/).
// A path outside every git work tree passes.
func CheckNotCommittable(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	top := enclosingRepo(filepath.Dir(abs))
	if top == "" {
		return nil
	}
	git, err := exec.LookPath("git")
	if err != nil {
		return fmt.Errorf("%s is inside the git repo %s and git is not installed to confirm it is ignored; "+
			"move security.key_dir outside the repo", abs, top)
	}
	cmd := exec.Command(git, "-C", top, "check-ignore", "-q", "--no-index", "--", abs)
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return nil // ignored
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		return fmt.Errorf("%s is inside the git repo %s and is NOT git-ignored, so the private key could be committed; "+
			"add its directory to that repo's .gitignore or choose another security.key_dir", abs, top)
	default:
		return fmt.Errorf("cannot confirm %s is git-ignored in %s: %w", abs, top, err)
	}
}

// enclosingRepo returns the nearest directory at or above dir that holds .git (a directory or, for worktrees and
// submodules, a file), or "" when there is none.
func enclosingRepo(dir string) string {
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Lstat(filepath.Join(d, ".git")); err == nil {
			return d
		}
		if filepath.Dir(d) == d {
			return ""
		}
	}
}
