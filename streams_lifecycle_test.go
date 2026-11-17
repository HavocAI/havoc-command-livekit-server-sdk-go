package lksdk

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	protoLogger "github.com/livekit/protocol/logger"
	"github.com/stretchr/testify/require"
)

type lifecycleTestWriter struct {
	close func()
	done  <-chan struct{}
}

var lifecycleTestStreams = []struct {
	name string
	new  func(*LocalParticipant) lifecycleTestWriter
}{
	{"bytes", func(p *LocalParticipant) lifecycleTestWriter {
		w := p.StreamBytes(StreamBytesOptions{})
		return lifecycleTestWriter{close: w.Close, done: w.closed.Watch()}
	}},
	{"text", func(p *LocalParticipant) lifecycleTestWriter {
		w := p.StreamText(StreamTextOptions{})
		return lifecycleTestWriter{close: w.Close, done: w.closed.Watch()}
	}},
}

func newStreamLifecycleTestEngine(t *testing.T) *RTCEngine {
	t.Helper()
	e := NewRTCEngine(false, nil, nil, nil)
	e.log = protoLogger.LogRLogger(logr.Discard())
	// These lifecycle tests do not connect a publisher. Fail header/trailer
	// publishing immediately instead of waiting for the normal connect timeout.
	e.connectionManager.incomingRequestParams.connectParams.ConnectTimeout = time.Nanosecond
	t.Cleanup(e.Close)
	return e
}

func requireLifecycleWriterClosed(t *testing.T, w lifecycleTestWriter) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream writer was not closed")
	}
}

func lifecycleRegistrySnapshot(e *RTCEngine) (length, capacity int, isNil bool, occupiedTail []bool) {
	e.onCloseLock.Lock()
	defer e.onCloseLock.Unlock()
	length, capacity, isNil = len(e.onClose), cap(e.onClose), e.onClose == nil
	for _, handler := range e.onClose[length:capacity] {
		occupiedTail = append(occupiedTail, handler != nil)
	}
	return
}

func TestStreamLifecycleCompletedWritersReleaseHandlers(t *testing.T) {
	for _, stream := range lifecycleTestStreams {
		t.Run(stream.name, func(t *testing.T) {
			e := newStreamLifecycleTestEngine(t)
			p := &LocalParticipant{engine: e}
			var publicCalls atomic.Int32
			e.OnClose(func() { publicCalls.Add(1) })
			var initialCapacity int
			for i := 0; i < 256; i++ {
				w := stream.new(p)
				length, capacity, _, _ := lifecycleRegistrySnapshot(e)
				require.Equal(t, 2, length, "one public callback and one open writer")
				if i == 0 {
					initialCapacity = capacity
				}

				w.close()
				w.close()
				requireLifecycleWriterClosed(t, w)

				length, capacity, _, occupiedTail := lifecycleRegistrySnapshot(e)
				require.Equal(t, 1, length, "completed writer must unregister")
				require.LessOrEqual(t, capacity, initialCapacity, "sequential streams must reuse registry storage")
				for _, occupied := range occupiedTail {
					require.False(t, occupied, "unused backing-array slots must not retain writers")
				}
			}
			require.Zero(t, publicCalls.Load(), "unregistering a writer must not invoke public callbacks")
			done := make(chan struct{})
			e.OnClose(func() { close(done) })
			e.Close()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("engine callbacks did not finish")
			}
			require.EqualValues(t, 1, publicCalls.Load())
		})
	}
}

func TestStreamLifecycleEngineClosesOpenWriters(t *testing.T) {
	e := newStreamLifecycleTestEngine(t)
	p := &LocalParticipant{engine: e}
	var writers []lifecycleTestWriter
	for _, stream := range lifecycleTestStreams {
		writers = append(writers, stream.new(p))
	}
	e.Close()
	e.Close()
	for _, writer := range writers {
		requireLifecycleWriterClosed(t, writer)
		writer.close()
	}
	_, _, isNil, _ := lifecycleRegistrySnapshot(e)
	require.True(t, isNil, "closing the engine must release the registry backing array")
	// A stream created after shutdown must immediately close itself too.
	for _, stream := range lifecycleTestStreams {
		requireLifecycleWriterClosed(t, stream.new(p))
	}
}

func TestStreamLifecycleConcurrentRegistrationAndClose(t *testing.T) {
	e := newStreamLifecycleTestEngine(t)
	p := &LocalParticipant{engine: e}
	const count = 64
	writers := make([]lifecycleTestWriter, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			w := lifecycleTestStreams[i%len(lifecycleTestStreams)].new(p)
			writers[i] = w
			if i%2 == 0 {
				// Race repeated explicit closes against the engine's callback.
				w.close()
				w.close()
			}
		}(i)
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			e.Close()
		}()
	}
	close(start)
	wg.Wait()
	for _, writer := range writers {
		requireLifecycleWriterClosed(t, writer)
	}
	length, _, _, _ := lifecycleRegistrySnapshot(e)
	require.Zero(t, length)
}

func TestStreamLifecyclePublicOnCloseCallbacks(t *testing.T) {
	e := newStreamLifecycleTestEngine(t)
	var order []int
	e.OnClose(func() { order = append(order, 1) })
	e.OnClose(func() { order = append(order, 2) })
	done := make(chan struct{})
	e.OnClose(func() { close(done) })
	e.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("engine callbacks did not finish")
	}
	require.Equal(t, []int{1, 2}, order)
	var calls int
	e.OnClose(func() {
		// Reentering the registry also verifies late callbacks run without its lock.
		e.OnClose(func() { calls++ })
		calls++
	})
	require.Equal(t, 2, calls, "late callbacks run immediately and exactly once")
}

func TestStreamLifecyclePublicRegistrationRacesEngineClose(t *testing.T) {
	e := newStreamLifecycleTestEngine(t)
	const count = 64
	var calls [count]atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			e.OnClose(func() { calls[i].Add(1) })
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		e.Close()
	}()
	close(start)
	wg.Wait()
	require.Eventually(t, func() bool {
		for i := range calls {
			if calls[i].Load() == 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Millisecond)
	for i := range calls {
		require.EqualValues(t, 1, calls[i].Load(), "callback %d", i)
	}
}
