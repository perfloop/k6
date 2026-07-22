package executor

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/lib"
)

func newActiveVUPoolBenchmarkState(tb testing.TB) *lib.ExecutionState {
	return lib.NewExecutionState(
		getTestRunState(tb, lib.Options{}, simpleRunner(func(context.Context, *lib.State) error {
			return nil
		})),
		mustNewExecutionTuple(nil, nil),
		0,
		0,
	)
}

func TestActiveVUPoolRunning(t *testing.T) {
	t.Parallel()

	const workers = 8

	state := newActiveVUPoolBenchmarkState(t)
	pool := newActiveVUPool(state)
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	ctx := context.Background()

	for range workers {
		pool.AddVU(ctx, nil, func(context.Context, lib.ActiveVU) bool {
			started <- struct{}{}
			<-release
			return true
		})
	}

	for range workers {
		require.Eventually(t, pool.TryRunIteration, time.Second, time.Millisecond)
	}
	for range workers {
		<-started
	}

	require.Equal(t, uint64(workers), pool.Running())
	require.Equal(t, int64(workers), state.GetCurrentlyActiveVUsCount())

	close(release)
	pool.Close()

	require.Zero(t, pool.Running())
	require.Zero(t, state.GetCurrentlyActiveVUsCount())
}

func BenchmarkActiveVUPoolDispatch(b *testing.B) {
	// The sealed command pins four Ps, so this creates one concurrent pool worker per P.
	workers := runtime.GOMAXPROCS(0)
	state := newActiveVUPoolBenchmarkState(b)
	pool := newActiveVUPool(state)
	ctx := context.Background()

	for range workers {
		pool.AddVU(ctx, nil, func(context.Context, lib.ActiveVU) bool {
			return true
		})
	}

	b.ResetTimer()
	start := time.Now()
	for b.Loop() {
		for !pool.TryRunIteration() {
			runtime.Gosched()
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	pool.Close()
	if active := state.GetCurrentlyActiveVUsCount(); active != 0 {
		b.Fatalf("active VUs after closing pool = %d, want 0", active)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "iterations/s")
}
