package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	redismock "github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

func removalFrame(workspaceID, userID string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"type": "member:removed", "payload": map[string]string{"workspace_id": workspaceID, "user_id": userID},
	})
	return raw
}

func TestMembershipRemovalLookupDoesNotBlockShardWithSubscribers(t *testing.T) {
	hub := NewHub()
	target := &Client{hub: hub, userID: testUserID, workspaceID: testWorkspaceID, send: make(chan []byte, 4)}
	hub.registerClient(target)
	other := attachRealtimeTestClient(hub, ScopeWorkspace, "other-workspace")
	other.workspaceID = "other-workspace"
	entered, release := make(chan struct{}, 1), make(chan struct{})
	hub.SetMembershipChecker(membershipFunc(func(context.Context, string, string) (bool, error) {
		entered <- struct{}{}
		<-release
		return false, nil
	}))
	rdb, mock := redismock.NewClientMock()
	t.Cleanup(func() { _ = rdb.Close() })
	relay := NewShardedStreamRelay(hub, rdb, rdb, ShardedStreamRelayConfig{Shards: 1, ReadCount: 2, ReadBlock: time.Millisecond})
	stream, lastID := ShardedStreamKey(0), "0-0"
	mock.ExpectXRead(&redis.XReadArgs{Streams: []string{stream, lastID}, Count: relay.config.ReadCount, Block: relay.config.ReadBlock}).SetVal([]redis.XStream{{
		Stream: stream, Messages: []redis.XMessage{
			{ID: "1-0", Values: envelopeRedisValues(envelope{Scope: ScopeWorkspace, ScopeID: testWorkspaceID, PayloadJSON: string(removalFrame(testWorkspaceID, testUserID))})},
			{ID: "2-0", Values: envelopeRedisValues(envelope{Scope: ScopeWorkspace, ScopeID: other.workspaceID, PayloadJSON: `{"type":"issue:updated"}`})},
		},
	}})
	finished := make(chan struct{})
	go func() { relay.readShardOnce(context.Background(), 0, stream, &lastID); close(finished) }()
	t.Cleanup(func() {
		close(release)
		<-finished
		waitFor(t, "removal completion", func() bool { return hub.memberRemovalPending.Load() == 0 })
		close(hub.memberRemovals)
		hub.removeClient(other)
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("removal lookup did not start")
	}
	select {
	case raw := <-other.send:
		if string(raw) != `{"type":"issue:updated"}` {
			t.Fatalf("unexpected unrelated event: %s", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribed workspace removal blocked unrelated shard traffic")
	}
	<-finished
	if lastID != "2-0" || target.stopped.Load() {
		t.Fatal("reader did not advance independently of the pending lookup")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipRemovalQueueOverflowDoesNotAllowContent(t *testing.T) {
	conn, peer := membershipSocketPair(t)
	hub := NewHub()
	target := &Client{hub: hub, conn: conn, userID: testUserID, workspaceID: testWorkspaceID, send: make(chan []byte, 4)}
	hub.registerClient(target)
	other := attachRealtimeTestClient(hub, ScopeWorkspace, "other-workspace")
	entered, release := make(chan struct{}, memberRemovalWorkers), make(chan struct{})
	hub.SetMembershipChecker(membershipBatchOverride{
		membershipFunc: membershipFunc(func(context.Context, string, string) (bool, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return true, nil // stale removal signals must not close rejoined connections
		}),
		batch: func(context.Context, []MembershipIdentity) (map[MembershipIdentity]bool, error) {
			return nil, nil // current content authorization sees the committed removal
		},
	})
	t.Cleanup(func() {
		close(release)
		waitFor(t, "saturated removal queue to drain", func() bool { return hub.memberRemovalPending.Load() == 0 })
		close(hub.memberRemovals)
		hub.removeClient(target)
		hub.removeClient(other)
	})
	raw := removalFrame(testWorkspaceID, testUserID)
	for i := 0; i < memberRemovalWorkers; i++ {
		hub.BroadcastToWorkspace(testWorkspaceID, raw)
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("removal worker did not start")
		}
	}
	for i := 0; i < memberRemovalQueueSize+1; i++ {
		hub.BroadcastToWorkspace(testWorkspaceID, raw)
	}
	if hub.memberRemovalDropped.Load() != 1 || hub.memberRemovalPending.Load() != memberRemovalQueueSize+memberRemovalWorkers {
		t.Fatal("removal queue did not enforce its bound")
	}
	hub.BroadcastToWorkspace("other-workspace", []byte(`{"type":"issue:updated"}`))
	if raw := <-other.send; string(raw) != `{"type":"issue:updated"}` {
		t.Fatalf("overflow stalled unrelated traffic: %s", raw)
	}
	if target.writeBatch([]byte(`{"type":"private-content"}`)) {
		t.Fatal("dropped removal signal permitted revoked content")
	}
	wantMembershipClosed(t, peer)
}

func TestMembershipQueuedRemovalKeepsRegistrationSnapshot(t *testing.T) {
	hub := NewHub()
	old := &Client{hub: hub, userID: testUserID, workspaceID: testWorkspaceID, send: make(chan []byte, 4)}
	hub.registerClient(old)
	entered, release := make(chan struct{}, memberRemovalWorkers), make(chan struct{})
	var releaseOnce sync.Once
	hub.SetMembershipChecker(membershipFunc(func(_ context.Context, userID, _ string) (bool, error) {
		if userID != testUserID {
			entered <- struct{}{}
			<-release
		}
		return false, nil
	}))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		waitFor(t, "queued removals to finish", func() bool { return hub.memberRemovalPending.Load() == 0 })
		close(hub.memberRemovals)
		hub.removeClient(old)
	})
	for i := 0; i < memberRemovalWorkers; i++ {
		hub.BroadcastToWorkspace(testWorkspaceID, removalFrame(testWorkspaceID, fmt.Sprintf("busy-user-%d", i)))
		<-entered
	}
	hub.BroadcastToWorkspace(testWorkspaceID, removalFrame(testWorkspaceID, testUserID))
	if len(hub.memberRemovals) != 1 {
		t.Fatal("target removal was not queued behind the blocked workers")
	}
	conn, peer := membershipSocketPair(t)
	current := &Client{hub: hub, conn: conn, userID: testUserID, workspaceID: testWorkspaceID, send: make(chan []byte, 4)}
	hub.registerClient(current)
	t.Cleanup(func() { hub.removeClient(current) })
	releaseOnce.Do(func() { close(release) })
	waitFor(t, "queued removal to be applied", func() bool { return hub.memberRemovalPending.Load() == 0 })
	if !old.stopped.Load() || current.stopped.Load() || len(current.send) != 0 {
		t.Fatal("queued signal inherited a later registration")
	}
	// This late registration has a stale handshake. It was not in the signal's
	// snapshot, but its own first content write must still deny and close it.
	if current.writeBatch([]byte(`{"type":"private-content"}`)) {
		t.Fatal("late registration bypassed current write authorization")
	}
	wantMembershipClosed(t, peer)
}

func TestMembershipRemovalClosesResourceOnlySubscriber(t *testing.T) {
	hub := NewHub()
	mc := &changingMembership{}
	conn := membershipConn(t, membershipServer(t, hub, mc), testUserID, testWorkspaceID)
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "payload": map[string]string{"scope": ScopeTask, "id": "resource"}}); err != nil {
		t.Fatal(err)
	}
	wantMembershipFrame(t, conn, "subscribe_ack")
	if err := conn.WriteJSON(map[string]any{"type": "unsubscribe", "payload": map[string]string{"scope": ScopeWorkspace, "id": testWorkspaceID}}); err != nil {
		t.Fatal(err)
	}
	wantMembershipFrame(t, conn, "unsubscribe_ack")
	if hub.HasLocalSubscribers(ScopeWorkspace, testWorkspaceID) {
		t.Fatal("workspace subscription was not removed")
	}
	mc.revoked.Store(true)
	hub.BroadcastToWorkspace(testWorkspaceID, removalFrame(testWorkspaceID, testUserID))
	wantMembershipClosed(t, conn)
	if hub.HasLocalSubscribers(ScopeTask, "resource") {
		t.Fatal("revocation skipped the resource-only connection")
	}
}
