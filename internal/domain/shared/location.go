package shared

import "strings"

// locationCodeSegments is the number of hyphen-joined segments of a
// facility-layout LocationCode (site-area-zone-aisle-bay-level-position).
const locationCodeSegments = 7

// SiteOfLocationCode returns the site segment (the first of seven) of a
// well-formed facility-layout LocationCode, and false for anything else: a
// code with any other number of hyphen-joined segments, or with an empty
// segment, has no determinable site. It is the same rule inventory-storage
// applies to a bin id (its ADR 0039); it is a pure string rule, never a
// lookup into another context.
func SiteOfLocationCode(code string) (string, bool) {
	parts := strings.Split(code, "-")
	if len(parts) != locationCodeSegments {
		return "", false
	}
	for _, p := range parts {
		if p == "" {
			return "", false
		}
	}
	return parts[0], true
}
