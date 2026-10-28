package brokerctl

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"time"
)

// Reachable reports whether something accepts TCP connections at host:port.
func Reachable(host string, port int) bool {
	c, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 500*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func bytesLines(b []byte) []string { return strings.Split(string(bytes.TrimSpace(b)), "\n") }
func contains(s, sub string) bool  { return strings.Contains(s, sub) }
func trim(s string) string         { return strings.Join(strings.Fields(s), " ") }
