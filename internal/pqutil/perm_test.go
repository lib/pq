//go:build !windows && !plan9

package pqutil

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

type statWrapper struct{ stat syscall.Stat_t }

func (stat_t *statWrapper) Name() string       { return "pem.key" }
func (stat_t *statWrapper) Size() int64        { return int64(100) }
func (stat_t *statWrapper) Mode() os.FileMode  { return os.FileMode(stat_t.stat.Mode) }
func (stat_t *statWrapper) ModTime() time.Time { return time.Now() }
func (stat_t *statWrapper) IsDir() bool        { return true }
func (stat_t *statWrapper) Sys() any           { return &stat_t.stat }

func TestSSLKeyPermissions(t *testing.T) {
	currentUID := uint32(os.Getuid())
	currentGID := uint32(os.Getgid())

	tests := []struct {
		stat    syscall.Stat_t
		wantErr string
	}{
		// user-owned: at most 0o600
		{syscall.Stat_t{Mode: 0o600, Uid: currentUID, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o400, Uid: currentUID, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o000, Uid: currentUID, Gid: currentGID}, ""},

		{syscall.Stat_t{Mode: 0o700, Uid: currentUID, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o640, Uid: currentUID, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o660, Uid: currentUID, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o604, Uid: currentUID, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o606, Uid: currentUID, Gid: currentGID}, "private key has world access"},

		// root-owned: at most 0o640
		{syscall.Stat_t{Mode: 0o600, Uid: 0, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o400, Uid: 0, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o040, Uid: 0, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o640, Uid: 0, Gid: currentGID}, ""},
		{syscall.Stat_t{Mode: 0o000, Uid: 0, Gid: currentGID}, ""},

		{syscall.Stat_t{Mode: 0o060, Uid: 0, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o006, Uid: 0, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o004, Uid: 0, Gid: currentGID}, "private key has world access"},
		{syscall.Stat_t{Mode: 0o666, Uid: 0, Gid: currentGID}, "private key has world access"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := checkPermissions(&statWrapper{stat: tt.stat})
			if !pqtest.ErrorContains(have, tt.wantErr) {
				t.Errorf("\nhave: %s\nwant: %s", have, tt.wantErr)
			}
		})
	}
}
