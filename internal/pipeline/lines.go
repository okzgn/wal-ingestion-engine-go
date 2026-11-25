package pipeline

import (
	"bufio"
	"errors"
	"io"
)

// lineReader reads newline-terminated records of any length without ever
// failing on a long one (unlike bufio.Scanner, which aborts the whole file with
// ErrTooLong). A line longer than max is reported as tooLong with its first max
// bytes kept, so a single bad record can never take its neighbours down with it.
// It also reports where the line sits in the file, so the caller can copy an
// oversized record whole to the dead-letter file without holding it in memory.
type lineReader struct {
	br  *bufio.Reader
	max int
	buf []byte
	off int64 // bytes consumed from the underlying reader so far

	// start and size locate the line returned by the last call to next: the
	// offset of its first byte and its full length without the terminator
	// (which, for a tooLong line, is more than len(line)).
	start, size int64
}

func newLineReader(r io.Reader, max int) *lineReader {
	return &lineReader{br: bufio.NewReaderSize(r, 64<<10), max: max}
}

// next returns the next line without its terminator. The slice is only valid
// until the following call. For a tooLong line it holds the first max bytes.
// terminated is false for a final line with no '\n', which is the signature of
// a torn write. It returns io.EOF when there are no more lines.
func (lr *lineReader) next() (line []byte, tooLong, terminated bool, err error) {
	lr.buf = lr.buf[:0]
	lr.start, lr.size = lr.off, 0
	started := false
	for {
		chunk, rerr := lr.br.ReadSlice('\n')
		if len(chunk) > 0 {
			started = true
			lr.off += int64(len(chunk))
			data := chunk
			term := chunk[len(chunk)-1] == '\n'
			if term {
				data = chunk[:len(chunk)-1]
			}
			lr.size += int64(len(data))
			if !tooLong {
				if room := lr.max - len(lr.buf); len(data) > room {
					tooLong = true
					data = data[:room]
				}
				lr.buf = append(lr.buf, data...)
			}
			if term {
				return lr.buf, tooLong, true, nil
			}
		}
		switch {
		case rerr == nil, errors.Is(rerr, bufio.ErrBufferFull):
			continue
		case errors.Is(rerr, io.EOF):
			if started {
				return lr.buf, tooLong, false, nil
			}
			return nil, false, false, io.EOF
		default:
			return nil, false, false, rerr
		}
	}
}
