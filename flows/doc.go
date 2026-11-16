// Package flows is a durable workflow engine with saga compensation, embedded
// in the application and backed by Postgres.
//
// Workflows are plain Go functions that are replayed deterministically from
// their event history; activities are the side-effecting calls they make,
// each optionally paired with a compensator that undoes it. When a workflow
// fails, the engine runs the compensators of the completed activities in
// reverse order, durably, without needing the workflow function.
//
// Several instances of the application can run the engine on the same
// database: every write is fenced by a lease version, and all lease and timer
// arithmetic uses the database clock.
package flows
