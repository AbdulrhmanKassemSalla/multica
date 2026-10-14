package realtime

const (
	memberRemovalWorkers   = 2
	memberRemovalQueueSize = 64
)

type memberRemovalJob struct {
	workspaceID string
	userID      string
	checker     MembershipChecker
	clients     []*Client
	subscribers map[*Client]bool
	notice      []byte
	eventID     string
}

func (h *Hub) snapshotMemberRemoval(workspaceID, userID string) memberRemovalJob {
	h.mu.RLock()
	defer h.mu.RUnlock()
	job := memberRemovalJob{workspaceID: workspaceID, userID: userID, checker: h.membership}
	room := h.rooms[sk(ScopeWorkspace, workspaceID)]
	job.subscribers = make(map[*Client]bool, len(room))
	for client := range room {
		job.subscribers[client] = true
	}
	for client := range h.clients {
		if client.workspaceID == workspaceID && client.userID == userID {
			job.clients = append(job.clients, client)
		}
	}
	return job
}

// Removal signals are advisory invalidations. Their database revalidation
// must never block a Redis shard reader, even when this queue is saturated.
// Content writers independently enforce fresh primary-database membership.
func (h *Hub) queueMemberRemoval(workspaceID string, raw []byte, eventID string) bool {
	userID, removal := parseMemberRemoval(workspaceID, raw)
	if !removal {
		return false
	}
	if userID == "" {
		return true
	}
	// Freeze targets and recipients before queueing, not when a worker starts:
	// a later registration must not inherit this signal or lookup result.
	job := h.snapshotMemberRemoval(workspaceID, userID)
	if len(job.clients) == 0 && len(job.subscribers) == 0 {
		return true
	}
	job.notice, job.eventID = raw, eventID
	h.memberRemovalOnce.Do(func() {
		for i := 0; i < memberRemovalWorkers; i++ {
			go h.runMemberRemovals()
		}
	})
	h.memberRemovalPending.Add(1)
	select {
	case h.memberRemovals <- job:
	default:
		h.memberRemovalPending.Add(-1)
		h.memberRemovalDropped.Add(1)
		M.MembershipRemovalDroppedTotal.Add(1)
	}
	return true
}

func (h *Hub) runMemberRemovals() {
	for job := range h.memberRemovals {
		if h.disconnectMember(job) {
			// The target receives its best-effort notice only through stopClient.
			// Later registrations and unsubscribed recipients get no old notice.
			h.broadcastToScope(ScopeWorkspace, job.workspaceID, job.notice, job.eventID, job.userID, job.subscribers)
		}
		h.memberRemovalPending.Add(-1)
	}
}
