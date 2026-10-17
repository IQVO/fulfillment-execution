package shared_test

import (
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

func TestSiteOfLocationCode(t *testing.T) {
	tests := []struct {
		code     string
		wantSite string
		wantOK   bool
	}{
		{"WH1-FWD-AMB-A01-01-01-A", "WH1", true},
		{"S-1-2-3-4-5-6", "S", true},
		{"", "", false},
		{"WH1", "", false},
		{"WH1-FWD-AMB-A01-01-01", "", false},
		{"WH1-FWD-AMB-A01-01-01-A-X", "", false},
		{"-FWD-AMB-A01-01-01-A", "", false},
		{"WH1-FWD--A01-01-01-A", "", false},
		{"WH1-FWD-AMB-A01-01-01-", "", false},
	}
	for _, tt := range tests {
		site, ok := shared.SiteOfLocationCode(tt.code)
		if site != tt.wantSite || ok != tt.wantOK {
			t.Errorf("SiteOfLocationCode(%q) = (%q, %v), want (%q, %v)", tt.code, site, ok, tt.wantSite, tt.wantOK)
		}
	}
}
