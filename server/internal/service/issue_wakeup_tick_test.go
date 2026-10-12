package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// idleConditionRules adds n enabled condition rules whose condition never
// holds, all due and ordered ahead of everything else, the way a busy
// production scan looks to a freshly hinted rule. They are spread over issues
// to stay under the per-issue limit and carry the instruction "never".
func idleConditionRules(t *testing.T, f principalFixture, s *IssueWakeupService, agent string, n int) {
	t.Helper()
	const perIssue = 30
	for made := 0; made < n; made += perIssue {
		idleIssue := parseTestUUID(t, f.Issue(t, "Idle conditions"))
		f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", idleIssue)
		f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", idleIssue)
		f.Cleanup(t, "DELETE FROM activity_log WHERE issue_id=$1", idleIssue)
		seed := wakeCreate(t, f, s, idleIssue, WakeupInput{AgentID: agent, Kind: "event", Instruction: "never",
			Condition: condition(t, map[string]any{"type": "issue_field", "field": "status", "value": "blocked"})})
		cloneWakeup(t, f, seed.ID, min(perIssue, n-made)-1, false)
	}
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second', updated_at=now()-interval '1 hour' WHERE workspace_id=$1 AND instruction='never'", f.WorkspaceID)
}

// cloneWakeup copies a rule n times, optionally as turned-off rules.
func cloneWakeup(t *testing.T, f principalFixture, id pgtype.UUID, n int, off bool) {
	t.Helper()
	if n <= 0 {
		return
	}
	f.Exec(t, `INSERT INTO issue_wakeup
		SELECT (jsonb_populate_record(w, jsonb_build_object('id', gen_random_uuid())
			|| CASE WHEN $3 THEN jsonb_build_object('enabled', false, 'disabled_at', now()) ELSE '{}'::jsonb END)).*
		FROM issue_wakeup w, generate_series(1, $2::int) WHERE w.id=$1`, id, n, off)
}

// MUL-7933 / #9139: a status change behind more due condition checks than one
// batch used to wait several scheduler passes (minutes). The hinted rule must
// fire in the first pass, exactly once, after the coder's run has already
// ended, and the due checks must all still be visited in that pass.
func TestWakeupTickFiresStatusHintBehindManyDueChecks(t *testing.T) {
	f, s, issue, reviewer := conditionFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	idleConditionRules(t, f, s, reviewer, 2*wakeupTickBatch+30)

	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: reviewer, Kind: "event", Instruction: "Review it",
		Condition: condition(t, map[string]any{"type": "issue_field", "field": "status", "value": "in_review"})})
	// Not due by time and last in scan order: only its hint can reach it.
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()+interval '1 hour', updated_at=now() WHERE id=$1", w.ID)

	// The coder sets in_review from its run, and the run ends before any pass.
	var owner string
	f.QueryRow(t, "SELECT owner_id::text FROM agent WHERE id=$1", reviewer).Scan(&owner)
	coder := f.privateAgentOwnedBy(t, owner, "coder")
	run := f.Task(t, coder, testutil.Cols{"issue_id": issue, "status": "running", "started_at": testutil.Raw("now()"),
		"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + coder + "')")})
	if _, err := f.q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: issue, Status: "in_review", WorkspaceID: ws, SourceTaskID: parseTestUUID(t, run)}); err != nil {
		t.Fatal(err)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL AND event_type='issue.status_changed'", w.ID); n != 1 {
		t.Fatalf("pending status hints = %d, want 1", n)
	}
	f.Exec(t, "UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1", run)

	if err := s.TickWorkspaces(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if n := wakeRuns(t, f, w.ID); n != 1 {
		t.Fatalf("reviewer runs after one pass = %d, want 1", n)
	}
	// Every due check was visited in the same pass, across several batches.
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND instruction='never' AND next_fire_at<=now()", f.WorkspaceID); n != 0 {
		t.Fatalf("%d due condition checks were left for a later pass", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue t JOIN issue_wakeup w ON t.context->>'wakeup_id'=w.id::text WHERE w.workspace_id=$1 AND w.instruction='never'", f.WorkspaceID); n != 0 {
		t.Fatalf("unmet conditions started %d runs", n)
	}
	// Later passes do not fire the once rule again.
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1 AND next_fire_at IS NOT NULL", w.ID)
	if err := s.TickWorkspaces(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if n := wakeRuns(t, f, w.ID); n != 1 {
		t.Fatalf("reviewer runs after two passes = %d, want 1", n)
	}
}

// More rules with pending inputs than a whole batch and a full batch of due
// checks both drain in one pass: the input lane takes at most its share of
// each batch, the due lane the rest, and each rule is visited once.
func TestWakeupTickDrainsBothLanesInOnePass(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	idleConditionRules(t, f, s, agent, wakeupTickBatch)

	// More turned-off rules with pending inputs than a whole batch: dispatch
	// only drops their inputs. The lane still lists rules that are not
	// enabled, as it must for a stopped once rule's captured inputs.
	seed := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "noisy"})
	f.Exec(t, "UPDATE issue_wakeup SET enabled=false, disabled_at=now() WHERE id=$1", seed.ID)
	cloneWakeup(t, f, seed.ID, wakeupTickBatch+20-1, true)
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload)
		SELECT gen_random_uuid(),id,revision,'k','comment.created','{}' FROM issue_wakeup WHERE issue_id=$1`, issue)

	inputs, err := f.q.ListInputWakeups(ctx, db.ListInputWakeupsParams{WorkspaceIds: []pgtype.UUID{ws}, SkipIds: []pgtype.UUID{}, RowLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != wakeupTickBatch+20 {
		t.Fatalf("input lane lists %d rules, want %d (disabled rules with inputs included)", len(inputs), wakeupTickBatch+20)
	}
	if err = s.TickWorkspaces(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt r JOIN issue_wakeup w ON w.id=r.wakeup_id WHERE w.issue_id=$1 AND r.processed_at IS NULL", issue); n != 0 {
		t.Fatalf("%d inputs left pending after one pass", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND instruction='never' AND next_fire_at<=now()", f.WorkspaceID); n != 0 {
		t.Fatalf("%d due checks were crowded out", n)
	}
}
