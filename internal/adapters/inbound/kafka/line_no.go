package kafka

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

// lineNo is WorkReleased.line_no as decoded off the wire. It is tolerant of
// numbers that do not fit an int (e.g. 9223372036854775808 or 1e30): a plain
// int field would fail the whole payload decode, which is deterministic and
// would dead-letter the message. The use case treats anything above the
// 32-bit maximum as "line unknown", so an unrepresentable positive number is
// saturated to math.MaxInt64 (still out of range, hence ignored with a WARN)
// and any other unrepresentable number (non-integral, or hugely negative)
// decodes as 0 (absent). A non-number token is still a decode error.
type lineNo int64

func (l *lineNo) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if string(b) == "null" {
		return nil
	}
	if v, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		*l = lineNo(v)
		return nil
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil && !isRangeError(err) {
		return fmt.Errorf("line_no: %q is not a number", b)
	}
	switch {
	case f >= math.MaxInt64: // includes +Inf from a range error
		*l = lineNo(math.MaxInt64)
	case f == math.Trunc(f) && f > math.MinInt64:
		*l = lineNo(int64(f))
	default:
		*l = 0
	}
	return nil
}

// Int narrows to the use case's int with an explicit bound: anything
// negative is 0 (unknown) and anything above math.MaxInt32 is reported as
// math.MaxInt32+1, which the use case ignores (the exact oversized value is
// irrelevant to it).
func (l lineNo) Int() int {
	switch {
	case l < 0:
		return 0
	case l > math.MaxInt32:
		return math.MaxInt32 + 1
	default:
		return int(l)
	}
}

func isRangeError(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}
