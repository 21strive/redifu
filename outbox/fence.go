package outbox

import (
	"context"
	"errors"
	"net"

	"github.com/redis/go-redis/v9"
)

// The drain's fence (lease.go): a batch's writes reach Redis only while this build holds the lease
// under the value it last saw. They are recorded first — the handlers run their SQL and enqueue into
// a pipeline that sends nothing — and then sent in one MULTI/EXEC that WATCHes
// {redifu:lease}:build (commitBatch), on one connection held for that round trip alone. A WATCH
// opened before the handlers ran held a connection of the cache's small pool for each drain in
// flight through all its SQL, LockGlobal waits of up to five seconds included, and a burst of
// drains could take the whole pool from the reads.
//
// The fence is a single-node design: the WATCHed key and every key of the batch are sent on one
// connection, which a Redis Cluster would answer with MOVED for the keys of other slots.

// recorder hands out pipelines that only collect commands: its hook swallows every pipeline it is
// asked to send, and its client never dials.
var recorder = newRecorder()

func newRecorder() *redis.Client {
	client := redis.NewClient(&redis.Options{Addr: "redifucache-recorder.invalid:0", MaxRetries: -1})
	client.AddHook(recordOnly{})
	return client
}

var errRecorderSends = errors.New("redifucache: a recording pipeline sends nothing")

// recordOnly is the recorder's hook.
type recordOnly struct{}

func (recordOnly) DialHook(redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) { return nil, errRecorderSends }
}

func (recordOnly) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(context.Context, redis.Cmder) error { return errRecorderSends }
}

func (recordOnly) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error { return nil }
}

// recording is a pipeline whose commands are collected, not sent: Commands hands them over once.
type recording struct {
	redis.Pipeliner
}

func record() recording { return recording{recorder.Pipeline()} }

// Commands are the commands enqueued so far, in order; the pipeline is empty afterwards.
func (r recording) Commands(ctx context.Context) []redis.Cmder {
	cmds, _ := r.Pipeliner.Exec(ctx)
	return cmds
}

// commitBatch sends a drain batch's recorded writes in one MULTI/EXEC behind the lease fence, and
// reports errLeaseLost (another holder, or another epoch), errLeaseMissing (the lease keys are
// gone), redis.TxFailedErr (the lease changed between the check and EXEC) or a redisWriteError.
// The recorded commands carry their replies afterwards, as if they had been sent where they were
// enqueued.
func (s *Store) commitBatch(ctx context.Context, cmds []redis.Cmder) error {
	enqueue := func(pipe redis.Pipeliner) error {
		for _, cmd := range cmds {
			_ = pipe.Process(ctx, cmd)
		}
		return nil
	}
	sent := func(err error) error {
		if err == nil || errors.Is(err, redis.Nil) {
			return nil
		}
		if errors.Is(err, redis.TxFailedErr) || errors.Is(err, errLeaseLost) || errors.Is(err, errLeaseMissing) {
			return err
		}
		var already redisWriteError
		if errors.As(err, &already) {
			return err
		}
		return redisWriteError{err}
	}
	s.settleBuild()
	if s.lease == nil { // a store built for a test without the lease
		_, err := s.rdb.TxPipelined(ctx, enqueue)
		return sent(err)
	}
	for attempt := 0; ; attempt++ {
		err := s.rdb.Watch(ctx, func(fenced *redis.Tx) error {
			holder, err := fenced.Get(ctx, leaseBuildKey).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return redisWriteError{err}
			}
			if holder == "" {
				return errLeaseMissing
			}
			if holder != s.lease.holder() {
				return errLeaseLost
			}
			_, err = fenced.TxPipelined(ctx, enqueue)
			return err
		}, leaseBuildKey)
		// A lease gone with the keys (a flush, a Redis restart) is taken again at once; nothing of
		// the batch was sent before the check.
		if errors.Is(err, errLeaseMissing) && attempt == 0 && s.reclaimLease(ctx) {
			continue
		}
		return sent(err)
	}
}
