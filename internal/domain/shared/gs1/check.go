package gs1

// maxCheckPayload is the longest payload the GS1 mod-10 algorithm is defined for (SSCC: 17).
const maxCheckPayload = 17

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Mod10CheckDigit returns the GS1 check digit for a payload of 1 to 17 ASCII digits (the key
// WITHOUT its check digit). Starting from the rightmost payload digit, digits are weighted
// 3, 1, 3, 1, ...; the check digit brings the weighted sum up to the next multiple of ten.
func Mod10CheckDigit(payload string) (int, error) {
	n := len(payload)
	if n < 1 || n > maxCheckPayload || !allDigits(payload) {
		return 0, ErrInvalidCheckInput
	}
	sum := 0
	for i := 0; i < n; i++ {
		d := int(payload[n-1-i] - '0')
		if i%2 == 0 {
			d *= 3
		}
		sum += d
	}
	return (10 - sum%10) % 10, nil
}

// ValidMod10 reports whether s is 2 to 18 ASCII digits whose last digit is the correct
// check digit of the others.
func ValidMod10(s string) bool {
	n := len(s)
	if n < 2 {
		return false
	}
	want, err := Mod10CheckDigit(s[:n-1])
	if err != nil || !allDigits(s[n-1:]) {
		return false
	}
	return int(s[n-1]-'0') == want
}
