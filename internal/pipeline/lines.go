package pipeline

import (
	"bufio"
	"errors"
	"io"
)

// lineReader reads newline-terminated records of any length without ever
// failing on a long one (unlike bufio.Scanner, which aborts the whole file with
// ErrTooLong). A line longer than max is reported as tooLong and skipped, so a
// single bad record can never take its neighbours down with it.
type lineReader struct {
	br  *bufio.Reader
	max int
	buf []byte
}

func newLineReader(r io.Reader, max int) *lineReader {
	return &lineReader{br: bufio.NewReaderSize(r, 64<<10), max: max}
}

// next returns the next line without its terminator. The slice is only valid
// until the following call. For a tooLong line it holds the leading part that
// was kept. terminated is false for a final line with no '\n', which is the
// signature of a torn write. It returns io.EOF when there are no more lines.
func (lr *lineReader) next() (line []byte, tooLong, terminated bool, err error) {
	lr.buf = lr.buf[:0]
	started := false
	for {
		chunk, rerr := lr.br.ReadSlice('\n')
		if len(chunk) > 0 {
			started = true
			data := chunk
			term := chunk[len(chunk)-1] == '\n'
			if term {
				data = chunk[:len(chunk)-1]
			}
			if !tooLong {
				if len(lr.buf)+len(data) > lr.max {
					tooLong = true
				} else {
					lr.buf = append(lr.buf, data...)
				}
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
