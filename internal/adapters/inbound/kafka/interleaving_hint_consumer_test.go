package kafka

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
)

// hintCloudEvent builds a raw CloudEvents 1.0 structured-mode message of the
// given type, mirroring task-interleaving's pinned InterleavingHintIssued
// contract.
func hintCloudEvent(t *testing.T, id, typ, subject string, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/task-interleaving",
		"type":            typ,
		"subject":         subject,
		"time":            time.Now().UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:task-interleaving:events:InterleavingHintIssued:v1",
		"data":            data,
	})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

func validHintData() map[string]any {
	return map[string]any{
		"hint_id":         "hint-1",
		"station_id":      "STN-07",
		"site_id":         "SITE-1",
		"after_task_id":   "task-8a1f",
		"origin_location": "WH1-FWD-A01-01-01-A",
		"expires_at":      time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339Nano),
		"policy_version":  2,
		"candidates": []map[string]any{
			{"rank": 1, "task_id": "task-9c2e", "task_type": "PICK", "location": "WH1-FWD-A01-02-01-A", "detour_m": 4.5},
			{"rank": 2, "task_id": "task-9d10", "task_type": "REPLENISH", "location": "WH1-FWD-A02-01-01-A", "detour_m": 12.0},
		},
	}
}

func TestDecodeInterleavingHintIssued(t *testing.T) {
	raw := hintCloudEvent(t, "ce-1", TypeInterleavingHintIssued, "STN-07", validHintData())
	req, ok, err := decodeInterleavingHintIssued(raw)
	if err != nil || !ok {
		t.Fatalf("decode = (ok=%v, err=%v), want ok", ok, err)
	}
	if req.EventId != "ce-1" || req.Hint.HintId != "hint-1" || req.Hint.StationId != "STN-07" || req.Hint.SiteId != "SITE-1" {
		t.Fatalf("request = %+v", req)
	}
	if len(req.Hint.Candidates) != 2 || req.Hint.Candidates[0].Rank != 1 || req.Hint.Candidates[0].TaskId != "task-9c2e" ||
		req.Hint.Candidates[1].Rank != 2 || req.Hint.Candidates[1].TaskId != "task-9d10" {
		t.Fatalf("candidates = %+v", req.Hint.Candidates)
	}
	if req.Hint.ExpiresAt.IsZero() {
		t.Fatal("expires_at not decoded")
	}
}

func TestDecodeInterleavingHintIssued_IgnoresOtherTypes(t *testing.T) {
	raw := hintCloudEvent(t, "ce-2", "com.warehouse.wes.task-interleaving.stationtrip.InterleavingHintConsumed", "STN-07", validHintData())
	if _, ok, err := decodeInterleavingHintIssued(raw); ok || err != nil {
		t.Fatalf("other type = (ok=%v, err=%v), want ignored", ok, err)
	}
}

func TestDecodeInterleavingHintIssued_NotACloudEvent(t *testing.T) {
	if _, _, err := decodeInterleavingHintIssued([]byte(`{"hint_id":"x"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecodeInterleavingHintIssued_InvalidPayloads(t *testing.T) {
	cases := map[string]func(map[string]any){
		"missing hint_id":    func(d map[string]any) { delete(d, "hint_id") },
		"missing station_id": func(d map[string]any) { delete(d, "station_id") },
		"missing site_id":    func(d map[string]any) { delete(d, "site_id") },
		"missing expires_at": func(d map[string]any) { delete(d, "expires_at") },
		"empty candidates":   func(d map[string]any) { d["candidates"] = []map[string]any{} },
		"rank below 1": func(d map[string]any) {
			d["candidates"] = []map[string]any{{"rank": 0, "task_id": "t", "task_type": "PICK", "location": "L", "detour_m": 1.0}}
		},
		"blank task_id": func(d map[string]any) {
			d["candidates"] = []map[string]any{{"rank": 1, "task_id": "", "task_type": "PICK", "location": "L", "detour_m": 1.0}}
		},
		"bad expires_at": func(d map[string]any) { d["expires_at"] = "not-a-time" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			data := validHintData()
			mutate(data)
			raw := hintCloudEvent(t, "ce-bad", TypeInterleavingHintIssued, "STN-07", data)
			if _, _, err := decodeInterleavingHintIssued(raw); !errors.Is(err, ErrInvalidInterleavingHint) {
				t.Fatalf("err = %v, want ErrInvalidInterleavingHint", err)
			}
		})
	}
}
