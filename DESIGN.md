# Fault-Tolerant Centralized Mutex — Design Document

## Overview

Extension of Rene's centralized mutual exclusion implementation to handle message failures and coordinator fail-stop-return failures using a primary-backup protocol.

## Failure Model

| Failure Type | Handled | Approach |
|--------------|---------|----------|
| Message loss/omission | Yes | ACK + retransmission |
| Coordinator fail-stop-return | Yes | Primary-backup with election |
| Client fail-stop-return | Yes | Lease expiry + state cleanup |
| Byzantine failures | No | Out of scope |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Clients                              │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│   │ Node_1  │  │ Node_2  │  │ Node_3  │  │ Node_N  │       │
│   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
│        │            │            │            │             │
│        └────────────┴─────┬──────┴────────────┘             │
│                           │ UDP                             │
│                           ▼                                 │
│   ┌─────────────────────────────────────────────────────┐   │
│   │                 Coordinator Cluster                  │   │
│   │  ┌─────────┐    ┌──────────┐    ┌──────────┐        │   │
│   │  │ Primary │───▶│ Backup_1 │    │ Backup_2 │        │   │
│   │  │ (ID:100)│    │ (ID:99)  │    │ (ID:98)  │        │   │
│   │  └─────────┘    └──────────┘    └──────────┘        │   │
│   │       │              ▲               ▲               │   │
│   │       └──────────────┴───────────────┘               │   │
│   │              SYNC + COORD_HB                         │   │
│   └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

**Primary Coordinator:**
- Processes REQ/REL/HB from clients
- Maintains authoritative state (current_holder, queue, lease_expiry)
- Synchronously replicates state to backups before ACKing clients
- Sends heartbeats to backups
- Steps down if majority of backups unreachable

**Backup Coordinator:**
- Receives and stores replicated state
- Monitors primary via heartbeat timeout
- Initiates election on primary failure detection
- Queues client requests during election (does not process)

**Client Node:**
- Sends REQ to request critical section
- Sends HB to maintain lease while in critical section
- Sends REL to release critical section
- Handles coordinator address change on COORDINATOR broadcast
- Retries messages on ACK timeout

## Protocol Decisions

### Message Reliability (UDP)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| ACK strategy | ACK every message | Explicit success signal; easier to reason about |
| Retransmission | Timer-based, max 3 retries | Bounded failure detection time |
| Idempotency | Per-node sequence numbers | Duplicate detection without complex state |
| Timeout | 500ms per attempt | Suitable for LAN latency |

### State Replication

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Timing | Synchronous (blocking) | Guarantees backup has state before client proceeds |
| Scope | Every state change | Matches Topic 6 p.45 diagram; no state divergence |
| Backup count | k backups for k-fault tolerance | Requirement: tolerate k coordinator failures |

### Failure Detection

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Mechanism | Primary → Backup heartbeat | Backups are watchdogs; nodes use request timeout |
| Interval | 1 second | Frequent enough for quick detection |
| Timeout | 2.5 seconds (2.5× interval) | Tolerates one missed HB |

### Election

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Algorithm | Bully | Demonstrates understanding; deterministic winner |
| Trigger | Any node can trigger | Redundancy in detection; backups have highest IDs so always win |
| Consistency | Term numbers (Raft-style) | Fencing against stale coordinators |

### Failover Behavior

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Active lease | Remains valid | State is replicated; no reason to invalidate |
| During election | Freeze new grants, queue requests | Prevents inconsistency; minimal disruption |
| Post-election | Process queue in order | Maintains fairness |

### Discovery & Broadcast

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Initial discovery | Static config | One-time setup; avoids startup protocol complexity |
| New coordinator | UDP broadcast | Fast notification to all nodes |
| Requirement | Same subnet/VLAN | UDP broadcast doesn't cross routers |

### Split-Brain Prevention

| Mechanism | Purpose |
|-----------|---------|
| Term numbers | Fence stale coordinator messages |
| Majority quorum | Coordinator valid only with majority backup contact |
| Step-down | Coordinator resigns if isolated from backups |

## Message Protocol

### Client ↔ Coordinator

| Message | Fields | Direction |
|---------|--------|-----------|
| `REQ` | node_id, seq, term | Node → Coord |
| `GRANT` | term, lease_duration | Coord → Node |
| `REL` | node_id, seq, term | Node → Coord |
| `HB` | node_id, seq, term | Node → Coord |
| `ACK` | msg_type, seq, term | Both |
| `NACK` | msg_type, seq, reason | Both |

### Coordinator Cluster

| Message | Fields | Direction |
|---------|--------|-----------|
| `SYNC` | term, state_snapshot | Primary → Backup |
| `SYNC_ACK` | term | Backup → Primary |
| `COORD_HB` | term | Primary → Backup |
| `COORD_HB_ACK` | term | Backup → Primary |

### Election

| Message | Fields | Direction |
|---------|--------|-----------|
| `ELECTION` | candidate_id, proposed_term | Candidate → Higher IDs |
| `OK` | responder_id, term | Higher → Lower |
| `COORDINATOR` | coord_id, coord_addr, term | Winner → All (broadcast) |
| `STEP_DOWN` | term | Resigning coord → All |

## State Definition

```
CoordinatorState {
    term: int                           # Current term number
    role: PRIMARY | BACKUP | CANDIDATE  # Current role
    current_holder: str | None          # Node ID holding token
    lease_expiry: float | None          # Timestamp when lease expires
    queue: deque[(node_id, address)]    # Waiting nodes (FIFO)
    known_nodes: set[str]               # All discovered nodes
    last_seen_seq: dict[str, int]       # Per-node sequence for dedup
    backup_addrs: list[address]         # Known backup addresses
    backup_ack_count: int               # Backups confirming HB
}
```

## Invariants

1. **Mutual Exclusion:** At most one node has `current_holder` status at any time
2. **Term Monotonicity:** Term numbers only increase; never decrease
3. **Lease Validity:** `current_holder` is valid only if `lease_expiry > now()`
4. **Sync-Before-ACK:** Primary must receive SYNC_ACK from majority before ACKing client
5. **Majority Rule:** Coordinator must maintain majority backup contact to remain active
6. **FIFO Fairness:** Queue order determines grant order

## Network Requirements

- All nodes on same subnet (for UDP broadcast)
- UDP port 5000 open for coordinator
- UDP port 5001 open for inter-coordinator communication
- `SO_BROADCAST` socket option enabled
- No NAT between nodes
