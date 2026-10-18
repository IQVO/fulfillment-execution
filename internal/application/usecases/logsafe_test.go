package usecases

import "testing"

func TestLogSafe_StripsLineBreaksOnly(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"plain value is unchanged":        {"ST-01", "ST-01"},
		"LF is removed":                   {"ST-01\nINFO forged entry", "ST-01INFO forged entry"},
		"CR is removed":                   {"ST-01\rforged", "ST-01forged"},
		"CRLF is removed":                 {"ST-01\r\nforged", "ST-01forged"},
		"spaces and symbols are retained": {"a b/c:d", "a b/c:d"},
		"empty stays empty":               {"", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := logSafe(tc.in); got != tc.want {
				t.Fatalf("logSafe(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
