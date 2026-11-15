// Package shared holds value objects and domain events shared across the
// task, station, and package aggregates.
package shared

// TaskId identifies a Task aggregate.
type TaskId string

// StationId identifies a Station aggregate.
type StationId string

// PackageId identifies a Package aggregate.
type PackageId string

// BatchId identifies a workbatch.WorkBatch aggregate (ADR 0063): the PICK
// tasks one station holds for one walk. Disposable after completion.
type BatchId string

// OrderRef identifies the released work this task or package fulfills.
type OrderRef string
