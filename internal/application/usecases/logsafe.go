package usecases

import "strings"

// logSafe strips CR/LF from a value that originates in a request (a station
// id) or wraps one (an error) before it is written to a log line, so a
// crafted value cannot forge a separate log entry (CWE-117). It mirrors the
// HTTP adapter's sanitizeForLog; the use-case layer cannot import an adapter,
// so the one-line rule is restated here rather than shared.
func logSafe(s string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(s)
}
