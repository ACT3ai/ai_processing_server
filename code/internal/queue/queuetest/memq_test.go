package queuetest

import (
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

func TestMemContract(t *testing.T) {
	RunContract(t, func(*testing.T) queue.Queue { return NewMem() })
}
