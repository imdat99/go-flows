package flows

import (
	"fmt"
	"strings"
	"sync"

	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/text"
	"github.com/imdat99/go-flows/internal/worker"
)

// DefaultQueue is the queue of workflows and activities that name none.
const DefaultQueue = "default"

// registry holds the workflows and activities defined in this binary. It
// is global, like database/sql drivers: definitions are package-level
// values, every engine of the process runs them.
var registry = struct {
	sync.RWMutex
	workflows    map[wfKey]executor.Workflow
	activities   map[string]*worker.Activity // name
	compensators map[string]*worker.Activity // activity name
}{
	workflows:    map[wfKey]executor.Workflow{},
	activities:   map[string]*worker.Activity{},
	compensators: map[string]*worker.Activity{},
}

// reservedPrefix marks names the engine uses itself.
const reservedPrefix = "flows."

type wfKey struct {
	name    string
	version int32
}

// mustQueue panics on an invalid queue name (an empty one would never run).
func mustQueue(queue string) {
	if err := text.CheckName(ErrInvalidArgument, "queue name", queue); err != nil {
		panic(err)
	}
}

// checkDefinedName panics on an invalid name or a name defined twice: a
// definition is program structure, and a duplicate would make the engine
// run the wrong function for recorded history (GW-12: go-workflows derived
// names from functions and silently replaced duplicates).
func checkDefinedName(what, name string) {
	if err := text.CheckName(ErrInvalidArgument, what+" name", name); err != nil {
		panic(err)
	}
	if strings.HasPrefix(name, reservedPrefix) {
		panic(fmt.Sprintf("flows: %s name %q uses the reserved prefix %q", what, name, reservedPrefix))
	}
}

func registerWorkflow(wf executor.Workflow) {
	checkDefinedName("workflow", wf.Name)
	if wf.Version < 1 {
		panic(fmt.Sprintf("flows: workflow %q: version %d must be at least 1", wf.Name, wf.Version))
	}
	key := wfKey{wf.Name, wf.Version}
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.workflows[key]; ok {
		panic(fmt.Sprintf("flows: workflow %s@%d defined twice", wf.Name, wf.Version))
	}
	registry.workflows[key] = wf
}

func registerActivity(name string, a, comp *worker.Activity) {
	checkDefinedName("activity", name)
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.activities[name]; ok {
		panic(fmt.Sprintf("flows: activity %q defined twice", name))
	}
	registry.activities[name] = a
	if comp != nil {
		registry.compensators[name] = comp
	}
}

// workerRegistry adapts the registry to the worker.
type workerRegistry struct{}

func (workerRegistry) Workflow(name string, version int32) (executor.Workflow, bool) {
	registry.RLock()
	defer registry.RUnlock()
	wf, ok := registry.workflows[wfKey{name, version}]
	return wf, ok
}

func (workerRegistry) Activity(name string, compensate bool) (*worker.Activity, bool) {
	registry.RLock()
	defer registry.RUnlock()
	if compensate {
		a, ok := registry.compensators[name]
		return a, ok
	}
	a, ok := registry.activities[name]
	return a, ok
}
