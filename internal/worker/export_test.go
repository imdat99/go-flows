package worker

// EagerStats reports the worker's eager counters (T10.21): activities run
// eagerly, those that found no free slot, and those of them that missed
// while an activity claim held slots.
func (w *Worker) EagerStats() (launched, missed, missedInClaim int64) {
	return w.eagerStats.launched.Load(), w.eagerStats.missed.Load(), w.eagerStats.missedInClaim.Load()
}
