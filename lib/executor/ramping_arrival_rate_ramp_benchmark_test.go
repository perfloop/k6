package executor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/guregu/null.v3"

	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/lib/types"
	"go.k6.io/k6/v2/metrics"
)

func BenchmarkRampingArrivalRateRunRamp(b *testing.B) {
	b.Run("VUs100", func(b *testing.B) {
		engineOut := make(chan metrics.SampleContainer, 1024)
		var dropped float64
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for container := range engineOut {
				for _, sample := range container.GetSamples() {
					if sample.Metric.Name == metrics.DroppedIterationsName {
						dropped += sample.Value
					}
				}
			}
		}()

		var iterations atomic.Int64
		runner := simpleRunner(func(_ context.Context, _ *lib.State) error {
			iterations.Add(1)
			return nil
		})
		testRunState := getTestRunState(b, lib.Options{}, runner)
		es := lib.NewExecutionState(testRunState, mustNewExecutionTuple(nil, nil), 100, 100)
		ctx, cancel, executor, _ := setupExecutor(b, &RampingArrivalRateConfig{
			TimeUnit:  types.NullDurationFrom(time.Second),
			StartRate: null.IntFrom(100000),
			Stages: []Stage{{
				Duration: types.NullDurationFrom(250 * time.Millisecond),
				Target:   null.IntFrom(200000),
			}},
			PreAllocatedVUs: null.IntFrom(100),
			MaxVUs:          null.IntFrom(100),
		}, es)
		defer cancel()

		b.ResetTimer()
		err := executor.Run(ctx, engineOut)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		close(engineOut)
		<-drained

		successful := iterations.Load()
		b.ReportMetric(float64(successful)+dropped, "scheduled_attempts")
		b.ReportMetric(dropped, "dropped_iterations")
	})
}
