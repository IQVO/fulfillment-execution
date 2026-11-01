package http

import (
	"encoding/json"
)

type sealPackageRequest struct {
	StationId string   `json:"stationId"`
	Contents  []string `json:"contents"`

	// ShipInOwnContainer opts into ship-in-own-container (ADR 0056): the
	// order line may ship in the SKU's CASE. LineQuantity, then required, is
	// the order line quantity in eaches. Both are absent from every request
	// that predates the feature, which seals exactly as before.
	ShipInOwnContainer bool `json:"shipInOwnContainer,omitempty"`
	LineQuantity       *int `json:"lineQuantity,omitempty"`
}

// validate checks only that stationId (the claim owner) is present; an
// empty contents slice is a domain-level concern (pack.ErrNoScannedContents,
// mapped to 422), not a malformed request.
func (r sealPackageRequest) validate() string {
	if r.StationId == "" {
		return "stationId is required"
	}
	if r.ShipInOwnContainer {
		if r.LineQuantity == nil {
			return "lineQuantity is required when shipInOwnContainer is true"
		}
		if *r.LineQuantity < 1 {
			return "lineQuantity must be at least 1"
		}
	}
	return ""
}

type packageResponse struct {
	Id                string   `json:"id"`
	OrderRef          string   `json:"orderRef"`
	Status            string   `json:"status"`
	ScannedContents   []string `json:"scannedContents"`
	FragileHandling   bool     `json:"fragileHandling"`
	GiftWrapRequested bool     `json:"giftWrapRequested"`
	SortLane          string   `json:"sortLane"`

	// Present only on a package that ships in its own container (ADR 0056);
	// omitted from every ordinary package, whose JSON is unchanged.
	ShipInOwnContainer bool               `json:"shipInOwnContainer,omitempty"`
	ContainerSource    string             `json:"containerSource,omitempty"`
	Container          *containerResponse `json:"container,omitempty"`
}

// containerResponse is the own container's identity and measures: the case
// GTIN, millimetres and grams.
type containerResponse struct {
	Gtin     string `json:"gtin"`
	LengthMm int    `json:"lengthMm"`
	WidthMm  int    `json:"widthMm"`
	HeightMm int    `json:"heightMm"`
	WeightG  int    `json:"weightG"`
}

type runSlamRequest struct {
	ActualWeight   float64
	ExpectedWeight float64

	// hasActual / hasExpected record whether the JSON object carried the
	// key at all. apis/openapi.yaml lists both weights as required, so an
	// ABSENT weight is a 400; but it also documents that an explicit null
	// is accepted and treated as 0, which a plain float64 (or *float64)
	// cannot tell apart from absence. Tracking key presence keeps both
	// documented behaviours.
	hasActual   bool
	hasExpected bool
}

// UnmarshalJSON decodes the weights and remembers which keys were present.
func (r *runSlamRequest) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var weights struct {
		ActualWeight   float64 `json:"actualWeight"`
		ExpectedWeight float64 `json:"expectedWeight"`
	}
	if err := json.Unmarshal(data, &weights); err != nil {
		return err
	}
	r.ActualWeight, r.ExpectedWeight = weights.ActualWeight, weights.ExpectedWeight
	_, r.hasActual = raw["actualWeight"]
	_, r.hasExpected = raw["expectedWeight"]
	return nil
}

// validate reports the first weight missing from the request, or "".
func (r runSlamRequest) validate() string {
	switch {
	case !r.hasActual:
		return "actualWeight is required"
	case !r.hasExpected:
		return "expectedWeight is required"
	default:
		return ""
	}
}
