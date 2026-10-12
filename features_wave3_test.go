package main_test

// Wave-3 step definitions: plain GET requests, numeric and list assertions
// on the last response, and the readiness gate. They reuse the shared world,
// so everything is still asserted through the REST boundary.

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"
)

func (w *world) aGetIsSentTo(path string) error {
	return w.sendRaw(http.MethodGet, path, "")
}

func (w *world) aPostIsSentWithoutBody(path string) error {
	return w.sendRaw(http.MethodPost, path, "")
}

func (w *world) responseFieldIsNumber(field string, want int) error {
	obj, err := w.responseObject()
	if err != nil {
		return err
	}
	got, ok := obj[field].(float64)
	if !ok || int(got) != want {
		return fmt.Errorf("expected response field %q to be %d, got %v: %s", field, want, obj[field], w.body)
	}
	return nil
}

func (w *world) responseIsListOf(want int) error {
	var list []any
	if err := json.Unmarshal(w.body, &list); err != nil {
		return fmt.Errorf("response is not a JSON array: %q", w.body)
	}
	if len(list) != want {
		return fmt.Errorf("response lists %d entries, want %d: %s", len(list), want, w.body)
	}
	return nil
}

func (w *world) shutdownBegins() error {
	w.readiness.SetNotReady()
	return nil
}

func (w *world) registerWave3Steps(sc *godog.ScenarioContext) {
	sc.Step(`^a GET is sent to "([^"]*)"$`, w.aGetIsSentTo)
	sc.Step(`^a POST is sent to "([^"]*)" without a body$`, w.aPostIsSentWithoutBody)
	sc.Step(`^the response field "([^"]*)" is the number (\d+)$`, w.responseFieldIsNumber)
	sc.Step(`^the response is a list of (\d+) entries$`, w.responseIsListOf)
	sc.Step(`^the service begins graceful shutdown$`, w.shutdownBegins)
}
