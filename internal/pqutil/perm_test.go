//go:build !windows && !plan8

package pqutil

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

type stat_t_wrapper struct{ stat syscall.Stat_t }

func (stat_t *stat_t_wrapper) Name() string       { return "pem.key" }
func (stat_t *stat_t_wrapper) Size() int64        { return int64(100) }
func (stat_t *stat_t_wrapper) Mode() os.FileMode  { return os.FileMode(stat_t.stat.Mode) }
func (stat_t *stat_t_wrapper) ModTime() time.Time { return time.Now() }
func (stat_t *stat_t_wrapper) IsDir() bool        { return true }
func (stat_t *stat_t_wrapper) Sys() any           { return &stat_t.stat }

func TestSSLKeyPermissions(t *testing.T) {
	currentUID := uint32(os.Getuid())
	currentGID := uint32(os.Getgid())

	tests := []struct {
		stat    syscall.Stat_t
		wantErr string
	}{
		{syscall.Stat_t{Mode: 0600, Uid: currentUID, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0640, Uid: 0, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0666, Uid: currentUID, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0666, Uid: 0, Gid: currentGID}, "private key has world access"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := checkPermissions(&stat_t_wrapper{stat: tt.stat})
			if !pqtest.ErrorContains(have, tt.wantErr) {
				t.Errorf("\nhave: %s\nwant: %s", have, tt.wantErr)
			}
		})
	}
}
