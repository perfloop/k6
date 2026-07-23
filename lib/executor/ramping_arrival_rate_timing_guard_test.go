package executor

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v3"

	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/lib/types"
	"go.k6.io/k6/v2/metrics"
)

const (
	rampTimingDuration = 250 * time.Millisecond
	rampTimingStart    = 100
	rampTimingTarget   = 200
	rampTimingVUs      = 1
	rampTimingMaxRuns  = 50
)

func TestRampingArrivalRateRunDoesNotDispatchAfterCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		var starts atomic.Int64
		runner := simpleRunner(func(ctx context.Context, _ *lib.State) error {
			if starts.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
			return nil
		})
		config := &RampingArrivalRateConfig{
			BaseConfig: BaseConfig{GracefulStop: types.NullDurationFrom(0)},
			TimeUnit:   types.NullDurationFrom(time.Second),
			StartRate:  null.IntFrom(1000),
			Stages: []Stage{{
				Duration: types.NullDurationFrom(time.Hour),
				Target:   null.IntFrom(1000),
			}},
			// An idle VU makes a post-cancellation dispatch observable as a
			// second runner invocation instead of a VU-pool drop.
			PreAllocatedVUs: null.IntFrom(2),
			MaxVUs:          null.IntFrom(2),
		}
		test := setupExecutorTest(t, "", "", lib.Options{}, runner, config)
		engineOut := make(chan metrics.SampleContainer, 100)
		done := make(chan error, 1)

		go func() {
			done <- test.executor.Run(test.ctx, engineOut)
		}()

		<-started
		test.cancel()
		require.NoError(t, <-done)
		synctest.Wait()
		require.Equal(t, int64(1), starts.Load())
	})
}

func TestRampingArrivalRateRunDoesNotDispatchAfterRegularDuration(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const duration = 20 * time.Millisecond

		var (
			dispatchesMu sync.Mutex
			dispatches   []time.Time
		)
		runner := simpleRunner(func(_ context.Context, _ *lib.State) error {
			dispatchesMu.Lock()
			dispatches = append(dispatches, time.Now())
			dispatchesMu.Unlock()
			return nil
		})
		config := &RampingArrivalRateConfig{
			BaseConfig: BaseConfig{GracefulStop: types.NullDurationFrom(duration)},
			TimeUnit:   types.NullDurationFrom(time.Second),
			StartRate:  null.IntFrom(1000),
			Stages: []Stage{{
				Duration: types.NullDurationFrom(duration),
				Target:   null.IntFrom(2000),
			}},
			PreAllocatedVUs: null.IntFrom(2),
			MaxVUs:          null.IntFrom(2),
		}
		test := setupExecutorTest(t, "", "", lib.Options{}, runner, config)
		engineOut := make(chan metrics.SampleContainer, 100)
		startedAt := time.Now()

		require.NoError(t, test.executor.Run(test.ctx, engineOut))
		synctest.Wait()

		dispatchesMu.Lock()
		defer dispatchesMu.Unlock()
		require.GreaterOrEqual(t, len(dispatches), 20)
		for _, dispatch := range dispatches {
			require.LessOrEqual(t, dispatch.Sub(startedAt), duration)
		}
	})
}

func BenchmarkRampingArrivalRateRunRampTiming(b *testing.B) {
	b.Run("VUs1", func(b *testing.B) {
		var (
			totalSuccessful int64
			totalDropped    int64
			lateness        = make([]int64, 0, b.N*rampTimingMaxRuns)
		)

		b.ResetTimer()
		b.StopTimer()
		for range b.N {
			engineOut := make(chan metrics.SampleContainer, 1024)
			var dropped atomic.Int64
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for container := range engineOut {
					for _, sample := range container.GetSamples() {
						if sample.Metric.Name == metrics.DroppedIterationsName {
							dropped.Add(int64(sample.Value))
						}
					}
				}
			}()

			// One VU serializes runner calls. Together with the zero-drop assertion
			// below, this makes the runner ordinal the scheduler timestamp ordinal.
			var iterations atomic.Int64
			dispatches := make([]int64, rampTimingMaxRuns)
			runner := simpleRunner(func(_ context.Context, _ *lib.State) error {
				index := iterations.Add(1) - 1
				if index < int64(len(dispatches)) {
					dispatches[index] = time.Now().UnixNano()
				}
				return nil
			})
			testRunState := getTestRunState(b, lib.Options{}, runner)
			es := lib.NewExecutionState(
				testRunState, mustNewExecutionTuple(nil, nil), rampTimingVUs, rampTimingVUs,
			)
			ctx, cancel, executor, _ := setupExecutor(b, &RampingArrivalRateConfig{
				TimeUnit:  types.NullDurationFrom(time.Second),
				StartRate: null.IntFrom(rampTimingStart),
				Stages: []Stage{{
					Duration: types.NullDurationFrom(rampTimingDuration),
					Target:   null.IntFrom(rampTimingTarget),
				}},
				PreAllocatedVUs: null.IntFrom(rampTimingVUs),
				MaxVUs:          null.IntFrom(rampTimingVUs),
			}, es)

			b.StartTimer()
			err := executor.Run(ctx, engineOut)
			b.StopTimer()
			cancel()
			if err != nil {
				b.Fatal(err)
			}
			close(engineOut)
			<-drained

			successful := iterations.Load()
			if successful > int64(len(dispatches)) {
				b.Fatalf("recorded %d dispatches, capacity is %d", successful, len(dispatches))
			}
			if dropped.Load() != 0 {
				b.Fatalf("recorded %d dropped iterations", dropped.Load())
			}
			if successful < 2 {
				b.Fatalf("recorded %d dispatches", successful)
			}
			firstDispatch := dispatches[0]
			firstScheduled := rampTimingScheduledAt(0)
			for index := int64(1); index < successful; index++ {
				delay := (dispatches[index] - firstDispatch) - int64(rampTimingScheduledAt(index)-firstScheduled)
				if delay > 0 {
					lateness = append(lateness, delay)
				} else {
					lateness = append(lateness, 0)
				}
			}
			totalSuccessful += successful
			totalDropped += dropped.Load()
		}

		if len(lateness) == 0 {
			b.Fatal("ramp run started no iterations")
		}
		elapsed := b.Elapsed()
		b.ReportMetric(float64(totalSuccessful)/elapsed.Seconds(), "iterations/s")
		b.ReportMetric(float64(totalSuccessful+totalDropped)/float64(b.N), "scheduled_attempts")
		b.ReportMetric(float64(totalDropped)/float64(b.N), "dropped_iterations")
		b.ReportMetric(float64(rampTimingP99(lateness)), "p99_relative_dispatch_lateness_ns")
	})
}

func rampTimingScheduledAt(index int64) time.Duration {
	from := float64(rampTimingStart) / float64(time.Second)
	to := float64(rampTimingTarget) / float64(time.Second)
	duration := float64(rampTimingDuration)
	i := float64(index + 1)
	x := (from*duration - noNegativeSqrt(duration*(from*from*duration+2*i*(to-from)))) / (from - to)
	return time.Duration(x)
}

func rampTimingP99(values []int64) int64 {
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values[(len(values)*99+99)/100-1]
}
