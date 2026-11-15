// Command flowsvet reports code that breaks the deterministic replay of
// go-flows workflows:
//
//	go vet -vettool=$(which flowsvet) ./...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/imdat99/go-flows/analyzer"
)

func main() { singlechecker.Main(analyzer.Analyzer) }
