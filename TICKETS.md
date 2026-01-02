# Fault-Tolerant Centralized Mutex — Implementation Tickets

## Epic Overview

| Epic | Owner | Description | Dependencies |
|------|-------|-------------|--------------|
| E1 | - | Message Reliability Layer | None |
| E2 | - | State Replication | E1 |
| E3 | - | Failure Detection | E1 |
| E4 | - | Leader Election | E1, E3 |
| E5 | - | Failover Handling | E2, E4 |
| E6 | - | Split-Brain Prevention | E4, E5 |
| E7 | - | Integration & Testing | All |

---

## E1: Message Reliability Layer

### T1.1: Define Message Format

**Scope:** Establish serialization format for all protocol messages.

**Tasks:**
- Define message structure with fields: type, node_id, seq, term, payload
- Choose serialization format (JSON for readability or struct packing for efficiency)
- Document byte layout or schema
- Ensure all message types from design doc are covered

**Acceptance Criteria:**
- All 14 message types have defined format
- Messages can be serialized and deserialized without data loss
- Format handles variable-length fields (e.g., state snapshots)

---

### T1.2: Implement Sequence Number Tracking

**Scope:** Add per-node sequence number generation and tracking.

**Tasks:**
- Client side: maintain local seq counter, increment on each new message
- Coordinator side: maintain `last_seen_seq[node_id]` dictionary
- Implement duplicate detection logic: reject if `seq <= last_seen_seq[node_id]`
- Handle wrap-around (if using fixed-size integers)

**Acceptance Criteria:**
- Duplicate messages are detected and ignored
- Retransmissions with same seq are correctly identified as duplicates
- New messages with incremented seq are processed normally

---

### T1.3: Implement ACK/NACK Handling

**Scope:** Add acknowledgment mechanism to all client-coordinator messages.

**Tasks:**
- Define ACK message structure: references original message type and seq
- Define NACK message structure: includes reason code (e.g., WRONG_TERM, INVALID_STATE)
- Coordinator sends ACK after processing REQ, REL, HB
- Client waits for ACK after sending each message

**Acceptance Criteria:**
- Every processed message generates an ACK
- Invalid messages generate appropriate NACK with reason
- ACK/NACK correctly references the original message

---

### T1.4: Implement Retransmission Logic

**Scope:** Add timeout-based retransmission for unacknowledged messages.

**Tasks:**
- Define timeout constant (500ms recommended)
- Define max retry count (3 recommended)
- Implement timer mechanism for pending messages
- On timeout: retransmit with same seq number
- After max retries: report communication failure to application layer
- Cancel timer on ACK receipt

**Acceptance Criteria:**
- Lost messages are automatically retransmitted
- Retransmissions stop after receiving ACK
- Max retry limit is enforced
- Communication failure is surfaced after exhausting retries

---

### T1.5: Refactor Existing Node Implementation

**Scope:** Update existing node.py to use reliability layer.

**Tasks:**
- Integrate sequence number generation into existing send logic
- Replace blocking recv with timeout-based recv
- Add ACK waiting after REQ, REL, HB sends
- Add retransmission loop around send operations
- Preserve existing state machine (IDLE → REQUEST → GRANTED → RELEASE)

**Acceptance Criteria:**
- Existing node functionality preserved
- Node handles message loss gracefully via retransmission
- Node detects coordinator unreachability after max retries

**Dependencies:** T1.1, T1.2, T1.3, T1.4

---

### T1.6: Refactor Existing Coordinator Implementation

**Scope:** Update existing coordinator to use reliability layer.

**Tasks:**
- Add sequence number tracking for all known nodes
- Send ACK after processing each client message
- Add duplicate detection before processing
- Update message parsing to handle new format

**Acceptance Criteria:**
- Coordinator correctly ACKs all valid messages
- Duplicate messages are rejected (idempotent behavior)
- Existing GRANT/queue logic unchanged

**Dependencies:** T1.1, T1.2, T1.3

---

## E2: State Replication

### T2.1: Define State Snapshot Structure

**Scope:** Define what coordinator state needs to be replicated.

**Tasks:**
- Identify all state variables: term, current_holder, lease_expiry, queue, known_nodes, last_seen_seq
- Define serializable snapshot format
- Determine snapshot size constraints (if any)
- Document which fields are required vs optional in snapshot

**Acceptance Criteria:**
- State snapshot contains all data needed for backup to take over
- Snapshot can be serialized/deserialized correctly
- Queue order is preserved in serialization

---

### T2.2: Implement SYNC Message Flow

**Scope:** Primary coordinator sends state updates to backups.

**Tasks:**
- Implement SYNC message construction with current state snapshot
- Implement SYNC sending to all known backup addresses
- Implement SYNC_ACK reception and tracking
- Define which events trigger SYNC: after processing REQ (grant or queue), REL, lease expiry

**Acceptance Criteria:**
- Every state-changing operation triggers SYNC to backups
- Primary tracks which backups have acknowledged
- SYNC contains complete current state

**Dependencies:** T2.1

---

### T2.3: Implement Backup State Reception

**Scope:** Backup coordinator receives and stores replicated state.

**Tasks:**
- Implement SYNC message parsing
- Replace local state with received state on valid SYNC
- Validate term number (reject SYNC from lower term)
- Send SYNC_ACK to primary after storing state

**Acceptance Criteria:**
- Backup state matches primary after SYNC
- Backup rejects SYNC from stale primary (lower term)
- SYNC_ACK sent only after state is durably stored

**Dependencies:** T2.1, T2.2

---

### T2.4: Implement Synchronous Replication

**Scope:** Primary waits for backup acknowledgment before ACKing client.

**Tasks:**
- After state change, send SYNC to backups
- Wait for SYNC_ACK from majority of backups (with timeout)
- Only after majority ACK: send ACK to client
- Handle timeout: either retry SYNC or mark backup as potentially failed

**Acceptance Criteria:**
- Client receives ACK only after state is replicated to majority
- Replication timeout does not block indefinitely
- Failed backup detection feeds into failure detection system

**Dependencies:** T2.2, T2.3

---

### T2.5: Implement Backup Coordinator Skeleton

**Scope:** Create backup coordinator process that can receive and store state.

**Tasks:**
- Create new backup coordinator module (or mode flag for existing coordinator)
- Implement UDP listener for SYNC and COORD_HB messages
- Implement state storage
- Implement SYNC_ACK and COORD_HB_ACK sending
- Add role tracking: BACKUP state (does not process client REQ directly)

**Acceptance Criteria:**
- Backup process can run alongside primary
- Backup maintains up-to-date copy of state
- Backup does not respond to client REQ messages (or responds with redirect)

**Dependencies:** T2.1, T2.3

---

## E3: Failure Detection

### T3.1: Implement Primary Heartbeat Sending

**Scope:** Primary coordinator sends periodic heartbeats to backups.

**Tasks:**
- Create background thread/task for heartbeat sending
- Send COORD_HB message to all backup addresses at fixed interval (1 second)
- Include current term in heartbeat
- Track COORD_HB_ACK responses from backups

**Acceptance Criteria:**
- Heartbeats sent at consistent interval
- Heartbeat includes current term
- ACK responses are tracked per backup

**Dependencies:** T1.1, T2.5

---

### T3.2: Implement Backup Heartbeat Monitoring

**Scope:** Backup monitors primary heartbeats and detects failure.

**Tasks:**
- Track timestamp of last received COORD_HB
- Implement timeout check (2.5 seconds recommended)
- On timeout: flag primary as potentially failed
- Send COORD_HB_ACK on each received heartbeat

**Acceptance Criteria:**
- Backup detects missing heartbeats within timeout window
- Single missed heartbeat does not trigger false positive
- Backup correctly responds to valid heartbeats

**Dependencies:** T3.1

---

### T3.3: Implement Client-Side Coordinator Failure Detection

**Scope:** Client nodes detect coordinator failure via request timeouts.

**Tasks:**
- After max retries on any message: flag coordinator as unreachable
- Enter "waiting for new coordinator" state
- Listen for COORDINATOR broadcast message
- On receiving COORDINATOR: update coordinator address, resume operation

**Acceptance Criteria:**
- Client detects coordinator failure after exhausting retries
- Client does not crash or hang on coordinator failure
- Client correctly handles coordinator address change

**Dependencies:** T1.4, T1.5

---

### T3.4: Implement Node Failure Detection (Coordinator Side)

**Scope:** Coordinator detects client node failures via lease expiry.

**Tasks:**
- Monitor lease_expiry timestamp continuously
- On lease expiry without HB or REL: clear current_holder, grant to next in queue
- Clean up node from queue if it fails to respond to GRANT (optional enhancement)
- Log node failure events for debugging

**Acceptance Criteria:**
- Expired leases are automatically cleared
- Next queued node receives GRANT after lease expiry
- State remains consistent after node failure

**Dependencies:** T1.6

---

## E4: Leader Election

### T4.1: Implement Term Number Management

**Scope:** Track and enforce term numbers across all components.

**Tasks:**
- Add term field to coordinator state
- Initialize term from persistent storage or 0 on first start
- Include term in all messages (client ↔ coord and coord ↔ coord)
- Validate incoming message terms: reject if term < current term
- Update local term if received message has higher term

**Acceptance Criteria:**
- All messages include valid term
- Stale messages (lower term) are rejected with NACK
- Term updates propagate correctly

**Dependencies:** T1.1

---

### T4.2: Implement Bully Election: ELECTION Message

**Scope:** Candidate sends ELECTION to higher-ID nodes.

**Tasks:**
- Define node ID assignment: backups get highest IDs (98, 99, 100)
- On detecting primary failure: construct ELECTION message with candidate ID and proposed term (current + 1)
- Send ELECTION to all nodes with higher ID
- Wait for OK responses with timeout

**Acceptance Criteria:**
- ELECTION message reaches all higher-ID nodes
- Proposed term is higher than current term
- Timeout prevents indefinite waiting

**Dependencies:** T3.2, T4.1

---

### T4.3: Implement Bully Election: OK Response

**Scope:** Higher-ID nodes respond to ELECTION and take over.

**Tasks:**
- On receiving ELECTION from lower-ID node: send OK response
- After sending OK: initiate own election (send ELECTION to even higher IDs)
- If no higher IDs exist: proceed to become coordinator

**Acceptance Criteria:**
- OK correctly signals "I will take over"
- Receiving OK causes original candidate to stand down
- Chain continues until highest available ID wins

**Dependencies:** T4.2

---

### T4.4: Implement Bully Election: COORDINATOR Announcement

**Scope:** Election winner broadcasts new coordinator status.

**Tasks:**
- After winning election (no OK received within timeout): increment term, become PRIMARY
- Broadcast COORDINATOR message to all known addresses (nodes + other backups)
- Include new coordinator ID, address, and term
- Use UDP broadcast for efficiency

**Acceptance Criteria:**
- COORDINATOR reaches all participants
- New term is established
- Winner transitions to PRIMARY role

**Dependencies:** T4.2, T4.3

---

### T4.5: Implement COORDINATOR Message Handling

**Scope:** All nodes handle coordinator announcement.

**Tasks:**
- On receiving COORDINATOR: validate term (must be > current)
- Update local coordinator address
- Update local term
- If was candidate: abort election, become BACKUP
- If was PRIMARY with lower term: step down to BACKUP

**Acceptance Criteria:**
- All nodes converge on same coordinator
- Stale COORDINATOR messages (lower term) ignored
- Role transitions happen correctly

**Dependencies:** T4.4

---

### T4.6: Handle Concurrent Elections

**Scope:** System correctly handles multiple simultaneous elections.

**Tasks:**
- Multiple nodes may detect failure and start elections simultaneously
- Term number ensures only one winner (highest term wins ties via highest ID)
- Nodes receiving ELECTION with higher proposed term: update term, restart election logic
- Ensure no split-brain from concurrent elections

**Acceptance Criteria:**
- Concurrent elections resolve to single winner
- No deadlock in election process
- Higher ID always wins among candidates with same term

**Dependencies:** T4.2, T4.3, T4.4, T4.5

---

## E5: Failover Handling

### T5.1: Implement Election Freeze Period

**Scope:** Coordinator candidates queue requests during election.

**Tasks:**
- During election (CANDIDATE state): do not process client REQ messages
- Store incoming REQ in temporary pending queue
- After election resolves: if became PRIMARY, process pending queue; if became BACKUP, discard (new primary handles)
- Respond to clients with temporary NACK (reason: ELECTION_IN_PROGRESS) or simply no response (client retries)

**Acceptance Criteria:**
- No GRANTs issued during election
- Requests are not lost (either queued or client retries)
- Post-election processing is correct

**Dependencies:** T4.4, T4.5

---

### T5.2: Implement State Takeover on Promotion

**Scope:** Backup becoming primary uses replicated state correctly.

**Tasks:**
- On winning election: transition from BACKUP to PRIMARY
- State already present from SYNC messages — no transfer needed
- Check lease_expiry: if expired, clear current_holder and grant to queue head
- Begin accepting client requests

**Acceptance Criteria:**
- New primary has correct state immediately
- Expired leases are handled on takeover
- No data loss during transition

**Dependencies:** T2.3, T4.4

---

### T5.3: Implement Active Lease Continuation

**Scope:** Node with active lease continues through coordinator failover.

**Tasks:**
- Node in critical section: continues working, lease timer keeps running
- On receiving COORDINATOR: update coordinator address for future HB/REL
- Send next HB or REL to new coordinator address
- New coordinator recognizes node as current_holder from replicated state

**Acceptance Criteria:**
- Active lease not invalidated by failover
- Node successfully releases to new coordinator
- No duplicate GRANT to same node

**Dependencies:** T5.2, T4.5

---

### T5.4: Implement Request Re-routing After Failover

**Scope:** Nodes waiting for GRANT correctly handle coordinator change.

**Tasks:**
- Node in REQUEST state (waiting for GRANT): may timeout during election
- On receiving COORDINATOR: re-send REQ to new coordinator
- New coordinator either: grants (if node was next in replicated queue) or queues
- Handle case where GRANT was in-flight during failover (idempotent handling)

**Acceptance Criteria:**
- Waiting nodes eventually receive GRANT from new coordinator
- Queue order preserved across failover
- No duplicate grants

**Dependencies:** T5.2, T4.5, T1.5

---

### T5.5: Implement Coordinator Restart Recovery

**Scope:** Previously failed coordinator rejoining the system.

**Tasks:**
- On restart: check for existing coordinator (listen for COORD_HB)
- If coordinator exists: join as BACKUP, request state sync
- If no coordinator found: initiate election
- Handle "bully" scenario: restarted node with highest ID takes over

**Acceptance Criteria:**
- Restarted node correctly rejoins cluster
- No disruption to ongoing operations if another coordinator is active
- State is synchronized before participating

**Dependencies:** T4.4, T2.3

---

## E6: Split-Brain Prevention

### T6.1: Implement Majority Quorum Tracking

**Scope:** Primary tracks backup connectivity for quorum.

**Tasks:**
- Track COORD_HB_ACK count within time window
- Define quorum: majority of total coordinator count (e.g., 2 of 3)
- If ACK count drops below quorum: set quorum_lost flag
- Feed into step-down logic

**Acceptance Criteria:**
- Primary accurately tracks backup connectivity
- Quorum threshold correctly calculated
- Quorum loss detected within bounded time

**Dependencies:** T3.1

---

### T6.2: Implement Coordinator Step-Down

**Scope:** Primary without quorum voluntarily resigns.

**Tasks:**
- On quorum loss detection: transition to STEP_DOWN state
- Broadcast STEP_DOWN message (best effort)
- Stop processing client requests
- Stop sending heartbeats
- Wait for COORDINATOR message or re-establish quorum

**Acceptance Criteria:**
- Isolated primary stops granting tokens
- STEP_DOWN notification sent
- Primary does not interfere with new election

**Dependencies:** T6.1

---

### T6.3: Implement Term Fencing in Message Processing

**Scope:** All components reject messages from stale terms.

**Tasks:**
- On every message receipt: compare message term with local term
- If message term < local term: reject with NACK (reason: STALE_TERM)
- If message term > local term: update local term (and potentially trigger state change)
- Log term mismatches for debugging

**Acceptance Criteria:**
- Stale primary cannot issue valid GRANTs after new election
- Delayed messages from old term are ignored
- Term updates are atomic

**Dependencies:** T4.1

---

### T6.4: Implement Network Partition Recovery

**Scope:** System correctly recovers when partition heals.

**Tasks:**
- Stepped-down old primary: on receiving higher-term COORDINATOR, become BACKUP
- On receiving COORD_HB from new primary: sync state and operate as backup
- Handle case: old primary had different queue state (new primary's state is authoritative)
- Log partition recovery events

**Acceptance Criteria:**
- Old primary correctly becomes backup
- No conflicting state after recovery
- Clients experience minimal disruption

**Dependencies:** T6.2, T2.3

---

## E7: Integration & Testing

### T7.1: Create Multi-Node Test Environment

**Scope:** Set up test infrastructure for distributed testing.

**Tasks:**
- Document how to run primary + 2 backups + 5 nodes across 2+ machines
- Create startup scripts with configurable IDs and addresses
- Implement logging with timestamps for debugging
- Set up network configuration (same subnet, firewall rules)

**Acceptance Criteria:**
- Full system can be started with documented commands
- Logs are captured and correlated across nodes
- Network broadcast works between all nodes

**Dependencies:** All implementation tickets

---

### T7.2: Test Message Loss Handling

**Scope:** Verify reliability layer handles message loss.

**Tasks:**
- Implement message drop simulation wrapper (configurable drop rate)
- Test: REQ with 50% drop rate still eventually granted
- Test: GRANT with drop still delivered via retransmission
- Test: SYNC with drop still replicated
- Measure: overhead of reliability layer (additional messages, latency)

**Acceptance Criteria:**
- System functions correctly with simulated message loss
- Retransmission count measurable
- No message type is single point of failure

**Dependencies:** E1

---

### T7.3: Test Coordinator Failover

**Scope:** Verify election and state transfer on primary failure.

**Tasks:**
- Test: kill primary process, verify backup takes over
- Test: node in critical section continues through failover
- Test: waiting nodes receive GRANT from new coordinator
- Test: queue order preserved across failover
- Measure: time from failure to new coordinator operational

**Acceptance Criteria:**
- Failover completes within bounded time
- No mutual exclusion violation during failover
- State correctly transferred

**Dependencies:** E4, E5

---

### T7.4: Test Split-Brain Scenarios

**Scope:** Verify system safety during network partitions.

**Tasks:**
- Simulate partition: isolate primary from backups (e.g., firewall rules)
- Verify: isolated primary steps down
- Verify: backup side elects new primary
- Verify: no two primaries issue GRANTs simultaneously
- Test partition healing: old primary becomes backup

**Acceptance Criteria:**
- At most one active primary at any time
- Mutual exclusion maintained during partition
- System recovers after partition heals

**Dependencies:** E6

---

### T7.5: Test Client Failure Handling

**Scope:** Verify system handles client crashes correctly.

**Tasks:**
- Test: client crashes while holding token → lease expires, next node granted
- Test: client crashes while in queue → queue cleaned up (or skip on grant failure)
- Test: client restarts → can request again with fresh state
- Measure: time from client failure to lease release

**Acceptance Criteria:**
- Client failure does not block system
- Lease expiry triggers correctly
- Restarted client operates normally

**Dependencies:** T3.4

---

### T7.6: Performance Measurement

**Scope:** Measure overhead of fault tolerance additions.

**Tasks:**
- Measure baseline: original system throughput (grants/second)
- Measure with reliability layer: throughput and latency
- Measure with replication: throughput and latency
- Compare election time across configurations
- Document cost of fault tolerance (messages per operation)

**Acceptance Criteria:**
- Performance numbers documented
- Overhead is quantified
- Comparison ready for presentation

**Dependencies:** E7.1, E7.2, E7.3

---

### T7.7: Prepare Demonstration

**Scope:** Create demo scenario for presentation.

**Tasks:**
- Script happy-path demo: nodes requesting and releasing
- Script failure demo: kill coordinator, show recovery
- Script split-brain demo: partition and recovery
- Prepare GUI dashboard for visualization (extend existing)
- Time demo to fit presentation slot

**Acceptance Criteria:**
- Demo runs reliably
- Failure scenarios clearly visible
- Recovery clearly demonstrated

**Dependencies:** All

---

## Ticket Summary by Assignee

| Assignee | Tickets | Focus Area |
|----------|---------|------------|
| TBD | T1.1–T1.4 | Message format & reliability primitives |
| TBD | T1.5, T1.6 | Node & coordinator refactoring |
| TBD | T2.1–T2.5 | State replication |
| TBD | T3.1–T3.4 | Failure detection |
| TBD | T4.1–T4.6 | Leader election |
| TBD | T5.1–T5.5 | Failover handling |
| TBD | T6.1–T6.4 | Split-brain prevention |
| TBD | T7.1–T7.7 | Integration & testing |

---

## Suggested Sprint Plan

| Sprint | Tickets | Goal |
|--------|---------|------|
| Sprint 1 | T1.1–T1.6 | Reliable messaging working |
| Sprint 2 | T2.1–T2.5, T3.1–T3.4 | Replication and detection working |
| Sprint 3 | T4.1–T4.6, T5.1–T5.5 | Election and failover working |
| Sprint 4 | T6.1–T6.4, T7.1–T7.7 | Split-brain prevention, testing, demo |

---

## Definition of Done (All Tickets)

- [ ] Implementation complete
- [ ] Manual testing performed
- [ ] Edge cases documented
- [ ] Logging added for debugging
- [ ] Code reviewed by at least one team member
- [ ] Integrated with main branch
- [ ] Demo scenario updated if applicable
