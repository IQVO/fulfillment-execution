// Package interleavinghintcopy is the local copy of task-interleaving's
// advisory hints (ADR 0051): one ACTIVE hint per station, written by the
// InterleavingHintIssued consumer (through usecases.ApplyInterleavingHint,
// deduped on the CloudEvents id in the same transaction) and read by
// claimNext as a tie-break inside one priority band. The copy is fail-open
// on both sides: an ignored hint costs nothing.
package interleavinghintcopy
