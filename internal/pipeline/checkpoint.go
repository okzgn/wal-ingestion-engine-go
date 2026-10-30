package pipeline

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	slotSize = 12 // 8 bytes line number + 4 bytes CRC32 of those 8 bytes
	slots    = 2
)

// Checkpoint stores "lines 1..N of this file are fully handled" in a tiny file
// that is overwritten in place (no rewrite, no rename).
//
// It keeps two checksummed slots and always overwrites the older one, so a torn
// or interrupted write can only damage the slot being written; the previous
// value stays valid. If both slots are unreadable the value is 0, which makes
// the whole file be reprocessed: safe under at-least-once delivery.
type Checkpoint struct {
	f    *os.File
	next int // slot the next Save writes to
}

// OpenCheckpoint opens (creating if needed) the checkpoint and returns its value.
func OpenCheckpoint(path string) (*Checkpoint, uint64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("open checkpoint: %w", err)
	}
	buf := make([]byte, slotSize*slots)
	n, _ := f.ReadAt(buf, 0) // a short read just yields invalid slots
	buf = buf[:n]

	var best uint64
	bestSlot := -1
	for s := 0; s < slots; s++ {
		lo, hi := s*slotSize, (s+1)*slotSize
		if hi > len(buf) {
			break
		}
		if v, ok := decodeSlot(buf[lo:hi]); ok && (bestSlot < 0 || v >= best) {
			best, bestSlot = v, s
		}
	}
	c := &Checkpoint{f: f}
	if bestSlot >= 0 {
		c.next = (bestSlot + 1) % slots
	}
	return c, best, nil
}

// Save durably records that the first line lines are handled.
func (c *Checkpoint) Save(line uint64) error {
	var b [slotSize]byte
	binary.LittleEndian.PutUint64(b[:8], line)
	binary.LittleEndian.PutUint32(b[8:], crc32.ChecksumIEEE(b[:8]))
	if _, err := c.f.WriteAt(b[:], int64(c.next*slotSize)); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := c.f.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	c.next = (c.next + 1) % slots
	return nil
}

// Close releases the file handle.
func (c *Checkpoint) Close() error { return c.f.Close() }

func decodeSlot(b []byte) (uint64, bool) {
	if len(b) != slotSize {
		return 0, false
	}
	if crc32.ChecksumIEEE(b[:8]) != binary.LittleEndian.Uint32(b[8:]) {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b[:8]), true
}
