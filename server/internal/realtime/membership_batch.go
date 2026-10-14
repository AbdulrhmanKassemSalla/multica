package realtime

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

const maxMembershipBatch = 256

type membershipWriteRequest struct {
	identity MembershipIdentity
	result   chan bool
}

func (h *Hub) authorizeWrites(identity MembershipIdentity) bool {
	h.writeMembershipOnce.Do(func() { go h.runMembershipChecks() })
	ctx, cancel := context.WithTimeout(context.Background(), membershipLookupTimeout+time.Second)
	defer cancel()
	request := membershipWriteRequest{identity: identity, result: make(chan bool, 1)}
	select {
	case h.writeMembership <- request:
	case <-ctx.Done():
		return false
	}
	select {
	case member := <-request.result:
		return member
	case <-ctx.Done():
		return false
	}
}

// Coalesce queued write batches across sockets into one indexed database
// query. Close the request cohort BEFORE starting the query: joining a lookup
// already in flight could authorize newly created content with an old snapshot.
func (h *Hub) runMembershipChecks() {
	for first := range h.writeMembership {
		requests := make([]membershipWriteRequest, 0, maxMembershipBatch)
		requests = append(requests, first)
		timer := time.NewTimer(time.Millisecond)
	collect:
		for len(requests) < maxMembershipBatch {
			select {
			case request, open := <-h.writeMembership:
				if !open {
					break collect
				}
				requests = append(requests, request)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		identities := make([]MembershipIdentity, 0, len(requests))
		seen := make(map[MembershipIdentity]bool, len(requests))
		for _, request := range requests {
			if !seen[request.identity] {
				identities = append(identities, request.identity)
				seen[request.identity] = true
			}
		}
		h.mu.RLock()
		checker := h.membership
		h.mu.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), membershipLookupTimeout)
		var members map[MembershipIdentity]bool
		var err error
		if checker == nil {
			err = errors.New("membership checker unavailable")
		} else {
			members, err = checker.Members(ctx, identities)
		}
		cancel()
		if err != nil {
			slog.Warn("ws batch membership lookup failed", "identities", len(identities), "error", err)
		}
		for _, request := range requests {
			request.result <- err == nil && members[request.identity]
		}
	}
}
