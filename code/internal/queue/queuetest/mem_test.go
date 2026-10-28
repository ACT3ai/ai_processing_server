package queuetest

import (
	"testing"
	"time"
)

func TestMemContract(t *testing.T) {
	Contract(t, New(time.Second), time.Second)
}
