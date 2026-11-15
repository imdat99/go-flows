// Package migrations embeds the goose migrations of the flows schema. A
// change is a new file: never edit a migration that has been applied.
package migrations

import "embed"

// FS holds the migrations.
//
//go:embed *.sql
var FS embed.FS
