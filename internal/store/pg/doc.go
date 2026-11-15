// Package pg is the Postgres implementation of store.Store, the engine's
// source of truth. It follows the saga engine's design: every write for a
// claimed task is fenced by owner and lease version on the locked row, all
// times come from Postgres now(), and a workflow task's checkpoint is one
// transaction.
package pg
