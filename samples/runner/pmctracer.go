package runner

import (
	"fmt"
	"sync"

	"gitlab.com/akita/akita/v3/sim"
	"gitlab.com/akita/akita/v3/tracing"
	"gitlab.com/akita/mem/v3/mem"
)

// pmcTracer can trace pmc activities.
type pmcTracer struct {
	sync.Mutex

	inflightTasks map[string]tracing.Task

	readCount       int
	writeCount      int
	readAvgLatency  sim.VTimeInSec
	writeAvgLatency sim.VTimeInSec
	readSize        uint64
	writeSize       uint64
}

func newpmcTracer() *pmcTracer {
	return &pmcTracer{
		inflightTasks: make(map[string]tracing.Task),
	}
}

// StartTask records the task start time
func (t *pmcTracer) StartTask(task tracing.Task) {
	t.Lock()
	defer t.Unlock()

	t.inflightTasks[task.ID] = task
}

// StepTask does nothing
func (t *pmcTracer) StepTask(task tracing.Task) {
	// Do nothing
}

// EndTask records the end of the task
func (t *pmcTracer) EndTask(task tracing.Task) {
	t.Lock()
	defer t.Unlock()

	originalTask, ok := t.inflightTasks[task.ID]
	if !ok {
		return
	}

	taskTime := task.EndTime - originalTask.StartTime
	if t.readCount != 0 && t.writeCount != 0 {
		switch originalTask.What {
		case "*mem.ReadReq":
			fmt.Println("pmc TRACER is called [case_read]")
			t.readAvgLatency = sim.VTimeInSec(
				(float64(t.readAvgLatency)*float64(t.readCount) +
					float64(taskTime)) / float64(t.readCount+1))
			t.readCount++
			t.readSize += originalTask.Detail.(*mem.ReadReq).AccessByteSize
		case "*mem.WriteReq":
			t.writeAvgLatency = sim.VTimeInSec(
				(float64(t.writeAvgLatency)*float64(t.writeCount) +
					float64(taskTime)) / float64(t.writeCount+1))
			t.writeCount++
			t.writeSize += uint64(len(originalTask.Detail.(*mem.WriteReq).Data))
		}
	}
	delete(t.inflightTasks, task.ID)
}
