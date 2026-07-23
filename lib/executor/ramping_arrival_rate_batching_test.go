package executor

import (
	"context"
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

func TestRampingArrivalRateRunStopsSchedulingWithIdleVUOnCancellation(t *testing.T) {
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
			// Keep a second VU ready so a post-cancellation scheduling attempt
			// becomes observable as another runner start.
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
