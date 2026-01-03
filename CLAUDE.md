# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a distributed critical section implementation using a centralized coordinator approach with lease-based mutual exclusion. The system demonstrates token-based access control in distributed systems where multiple nodes request exclusive access to a critical section through a single coordinator.

## Coding Standards

- **Style**: snake_case for all identifiers
- **Indentation**: Tabs (not spaces)
- **Comments**: None. Code must be self-explanatory through naming and structure
- **Typing**: Use Python type hints throughout (from typing import ...)
- **Conciseness**: Prefer compact, readable implementations
- **Updates**: After each change, update this CLAUDE.md file to reflect new state
- **Commands**: User runs external commands (e.g., `uv run critical/coordinator.py`)

## Architecture

### Core Components

1. **Coordinator System with PRIMARY/BACKUP Roles**: 
   - **Core Logic** (`critical/coordinator.py`): Enhanced with PRIMARY/BACKUP role support and SYNC-based state replication
   - **GUI Interface** (`critical/coordinator_gui.py`): Jupyter widget-based dashboard wrapper  
   - **Notebook Interface** (`critical/coordinator.ipynb`): Interactive Jupyter notebook demo
   - **Role-Based Architecture**:
     - PRIMARY: Processes client requests, maintains authoritative state, sends SYNC with majority consensus
     - BACKUP: Stores replicated state, redirects client requests with NACK, receives SYNC updates
     - CANDIDATE: Conducts election, buffers REQ messages, processes pending after becoming PRIMARY
   - **Dual Network Architecture**:
     - Port 50000: Client communication (REQ/REL/HB from nodes)
     - Port 50001: Inter-coordinator communication (SYNC/SYNC_ACK between coordinators)
   - **State Management**: CoordinatorState dataclass with backup health tracking and sequence counters
   - **StateSnapshot**: Serializable state container with to_dict()/from_dict() methods
   - **SYNC Consensus**: Majority-based replication with 1-second timeout and suspect backup marking
   - **Failover Handling**: Election freeze, pending request buffering, state takeover, active lease continuation
   - **Quorum Tracking**: Majority-based PRIMARY validity with automatic step-down on quorum loss
   - **Split-Brain Prevention**: Term validation, STEP_DOWN broadcasts, partition recovery
   - Protocol-aware message handling with ACK/NACK responses and term validation
   - Duplicate detection using per-node sequence number tracking
   - Term-based consensus support for Bully leader election
   - Thread-safe implementation with `threading.Lock()` for state synchronization
   - CLI arguments: --role primary|backup --id <int> --peers <ip:port,ip:port>

2. **Protocol Layer** (`critical/protocol.py`):
   - **Message Types**: 14 dataclasses covering all communication patterns
     - Client ↔ Coordinator: REQ, GRANT, REL, HB, ACK, NACK (6 types)
     - Coordinator Cluster: SYNC, SYNC_ACK, COORD_HB, COORD_HB_ACK (4 types)
     - Election: ELECTION, OK, COORDINATOR, STEP_DOWN (4 types)
   - **Serialization**: JSON-based encoding/decoding with UTF-8 transport
   - **ReliableSender**: UDP reliability layer with sequence numbers and retries
     - Per-destination sequence counters for message ordering
     - Duplicate detection via `last_seen_seq` tracking
     - Configurable timeout (0.5s) and retry count (3 attempts)
   - **Type Safety**: Full type hints with Python 3.14+ annotations

3. **Smart Node Template** (`critical/node.py`): 
   - Enhanced protocol implementation using structured messages from `protocol.py`
   - Full ACK/NACK handling with proper error logging and retry mechanisms
   - Term tracking for distributed consensus compatibility
   - ReliableSender integration for message delivery guarantees
   - Complete protocol state machine: IDLE → REQ+ACK → GRANT → HB+ACK → REL+ACK
   - **Failover Support**: Coordinator failure detection, COORDINATOR/STEP_DOWN message handling
   - **Active Lease Continuation**: Continue work during coordinator transitions
   - **Dynamic Coordinator Updates**: Update coordinator address from broadcasts during operation
   - Configured for coordinator IP `192.168.1.101`

4. **Simple Node Instances** (`critical/nodes/node1.py` through `node5.py`):
   - Minimalist node implementations with fixed IDs (`Node_1`, `Node_2`, etc.)
   - Basic REQ/REL protocol without heartbeat mechanism
   - No error handling or timeout management (blocking `recv()` calls)
   - Simplified for testing and demonstration purposes

### Protocol Specification

#### Message Format
Two protocol layers exist:

**Legacy Protocol** (simple string-based):
- **REQ {node_id}**: Node requests critical section access
- **REL {node_id}**: Node releases critical section
- **HB {node_id}**: Heartbeat to maintain lease
- **GRANT**: Coordinator grants access to requesting node

**Enhanced Protocol** (`critical/protocol.py`):
JSON-serialized messages with structured fields:
- **Common Fields**: `node_id`, `seq`, `term`, `type`
- **Client ↔ Coordinator**: REQ, GRANT(lease_duration), REL, HB, ACK(msg_type), NACK(msg_type, reason)
- **Coordinator Cluster**: SYNC(state_snapshot), SYNC_ACK, COORD_HB, COORD_HB_ACK
- **Election**: ELECTION(candidate_id, proposed_term), OK(responder_id), COORDINATOR(coord_id, coord_addr), STEP_DOWN
- **Reliability**: Sequence numbers for deduplication, automatic retries, ACK/NACK responses

#### State Management
- **current_holder**: Currently active node ID (None if no holder)
- **lease_expiry**: Timestamp when current lease expires
- **queue**: FIFO queue of (node_id, address, seq) tuples awaiting access
- **known_nodes**: Set of all nodes that have ever contacted coordinator
- **term**: Current consensus term for leader election compatibility
- **last_seen_seq**: Dict mapping node_id to last processed sequence number
- **backup_status**: Dict mapping backup addresses to "healthy"/"suspect" status
- **sync_seq_counter**: Incrementing sequence number for SYNC messages

### Network Configuration & Technical Details

#### Socket Configuration
- **Coordinator**: UDP socket bound to `0.0.0.0:50000`
  - `SO_REUSEADDR` enabled for rapid restart capability
  - Non-blocking operation with timeout for graceful shutdown
  - Handles `OSError` for port conflicts and socket cleanup

- **Nodes**: UDP client sockets with 2-second timeouts
  - Smart nodes: Retry logic with 1-second delays on timeout
  - Simple nodes: No timeout handling (blocking behavior)

#### Lease System Details
- **Lease Duration**: 5.0 seconds (configurable constant)
- **Heartbeat Interval**: 2.0 seconds (sent by smart nodes only)
- **Automatic Expiry**: Coordinator checks lease expiry every message and GUI update
- **Grace Period**: None - strict lease enforcement

#### Threading Architecture
- **UDP Server Thread**: Handles all network communication and protocol logic
- **GUI Update Thread**: 10 FPS dashboard updates (0.1-second intervals)
- **State Lock**: Single `threading.Lock()` protects all shared state variables
- **Daemon Threads**: Both threads marked as daemon for clean process termination

### GUI Dashboard Implementation

#### Real-time Visualization
- **Node Widgets**: Dynamic creation using `ipywidgets.VBox` containers
- **State Indicators**: Color-coded borders and labels
  - Green: Active token holder with progress bar showing remaining lease time
  - Orange: Queued nodes with position indicator
  - Gray: Idle nodes
- **Progress Bars**: Real-time countdown of remaining lease time
- **Grid Layout**: Responsive 4-column grid with 10px gaps

#### Widget Management
- **Dynamic Node Discovery**: Widgets created on-demand as nodes join
- **Cache System**: `node_widgets_cache` prevents widget recreation
- **State Synchronization**: GUI updates under state lock protection

## Development Commands

This is a Python project managed with `uv` package manager and `pyproject.toml`:

### Installation & Setup
```bash
# Install dependencies using uv
uv sync

# Alternative: Install directly with pip
pip install ipywidgets jupyter
```

### Running the System

1. **Start Coordinator with PRIMARY/BACKUP Roles**:
   ```bash
   # PRIMARY coordinator (processes client requests)
   uv run critical/coordinator.py --role primary --id 100 --peers 192.168.1.102:50000,192.168.1.103:50000
   
   # BACKUP coordinator (replicates state, redirects clients)
   uv run critical/coordinator.py --role backup --id 98 --peers 192.168.1.101:50000
   uv run critical/coordinator.py --role backup --id 99 --peers 192.168.1.101:50000
   
   # Legacy: Single coordinator (backwards compatibility)
   uv run critical/coordinator.py --role primary --id 100
   
   # Option 2: Jupyter notebook with interactive GUI dashboard
   jupyter notebook critical/coordinator.ipynb
   # Execute all cells in sequence to start the coordinator server
   # Dashboard will appear inline with real-time node status
   
   # Option 3: Use the GUI module programmatically
   uv run python -c "from critical.coordinator_gui import start_coordinator_gui; start_coordinator_gui()"
   ```

2. **Start Nodes**:
   ```bash
   # Simple nodes (basic REQ/REL only)
   uv run critical/nodes/node1.py
   uv run critical/nodes/node2.py
   uv run critical/nodes/node3.py
   uv run critical/nodes/node4.py
   uv run critical/nodes/node5.py
   
   # Smart node (with heartbeat and error handling)
   uv run critical/node.py
   ```

### Testing Strategy

Comprehensive test infrastructure located in `critical/tests/`:

#### Test Environment (`test_env.py`)
- **ProcessManager**: Automated startup/cleanup of coordinators and nodes with proper CLI arguments
- **NetworkConfig**: Configurable IP/port mapping for test isolation (PRIMARY:50000, BACKUP1:50002, BACKUP2:50004)
- **MessageDropWrapper**: Socket wrapper with configurable drop rate for network fault simulation
- **TimestampLogger**: Structured logging with ISO timestamps and event categorization
- **Standard Cluster**: 1 PRIMARY + 2 BACKUP coordinators + 3 simple nodes for consistent test setup

#### Test Scenarios (`scenarios.py`)
Automated test suite with 4 core fault tolerance tests:

1. **test_message_loss()**: 30% message drop rate tolerance
   - Applies network packet loss simulation
   - Verifies eventual consistency and system progress
   - Validates minimum grant throughput under adverse conditions

2. **test_coordinator_failover()**: PRIMARY coordinator failure recovery
   - Kills PRIMARY coordinator during operation
   - Verifies BACKUP promotion via Bully election algorithm  
   - Validates queue preservation and state continuity

3. **test_split_brain()**: Network partition and recovery
   - Simulates network partition by killing backup coordinator
   - Verifies STEP_DOWN behavior and term validation
   - Tests partition recovery and split-brain prevention

4. **test_client_failure()**: Client holding critical section fails
   - Kills node currently holding critical section token
   - Verifies lease expiration mechanism (5-second timeout)
   - Validates automatic grant to next queued node

#### Performance Benchmarking (`benchmark.py`)
Quantitative analysis of system performance characteristics:

- **baseline_throughput()**: Single coordinator grants/second measurement
- **replication_overhead()**: Performance impact of PRIMARY+BACKUP replication
- **election_timing()**: Coordinator failover latency measurement  
- **ComparisonTable**: Tabulated results with overhead percentages and statistical analysis

#### Interactive Demos (`demo.py`)
Visual demonstrations with real-time system state display:

- **happy_path_demo()**: Normal operation with nodes cycling through critical section
- **failure_injection_demo()**: Coordinator failure with visual recovery process
- **VisualDisplay**: Color-coded terminal output with node status and event logging
- **interactive_demo()**: Command-line interface for running specific demo scenarios

#### Manual Testing Commands
```bash
# Run full test suite
python critical/tests/scenarios.py

# Run individual tests
python critical/tests/scenarios.py message_loss
python critical/tests/scenarios.py coordinator_failover
python critical/tests/scenarios.py split_brain
python critical/tests/scenarios.py client_failure

# Performance benchmarking
python critical/tests/benchmark.py full
python critical/tests/benchmark.py baseline
python critical/tests/benchmark.py replication
python critical/tests/benchmark.py election

# Interactive demonstrations
python critical/demo.py interactive
python critical/demo.py happy
python critical/demo.py failure
```

#### Testing Requirements Validation
- **Mutual Exclusion**: Automated verification via grant tracking and concurrency detection
- **FIFO Ordering**: Queue position monitoring and order validation
- **Lease Expiry**: Precise timing measurements with 5-second lease enforcement
- **Heartbeat Functionality**: Smart node lease extension validation
- **Network Resilience**: Fault injection with configurable failure scenarios
- **Election Correctness**: Term validation, vote counting, and leadership transition timing

## Technical Implementation Details

### Coordinator System Architecture

#### Core Coordinator (`coordinator.py`)
- **Coordinator Class**: Reusable coordinator implementation with PRIMARY/BACKUP role support
- **CoordinatorState**: Centralized state management with SYNC-based replication and Bully election support
  - `term`: Current consensus term for distributed coordination
  - `role`: PRIMARY|BACKUP|CANDIDATE role designation
  - `current_holder`: String ID of token holder or `None`
  - `lease_expiry`: Float timestamp when current lease expires
  - `queue`: `collections.deque` storing `(node_id, address, seq)` tuples
  - `known_nodes`: Set of all discovered node IDs for GUI management
  - `last_seen_seq`: Dict tracking per-node sequence numbers for deduplication
  - `backup_addrs`: List of backup coordinator addresses for replication
  - `backup_status`: Dict tracking backup health ("healthy"/"suspect")
  - `sync_seq_counter`: Incrementing counter for SYNC message sequencing
  - `election_in_progress`: Boolean tracking active election state
  - `election_start_time`: Timestamp when current election started
  - `candidate_peers`: List of higher-ID peers contacted during election
  - `election_seq_counter`: Incrementing counter for ELECTION message sequencing
  - `pending_requests`: Deque buffering REQ messages during CANDIDATE state for FIFO processing
  - `quorum_ack_count`: Count of COORD_HB_ACK responses in current heartbeat round
  - `last_quorum_check`: Timestamp of last quorum check for PRIMARY validity
  - `quorum_lost_start`: Timestamp when quorum was first lost (None if quorum healthy)
- **StateSnapshot**: Serializable state container for SYNC message replication
- **Dual Threading Architecture**: 
  - `_client_server()`: Handles client traffic on port 50000
  - `_coord_server()`: Handles coordinator traffic on port 50001
- **Role-Based Message Processing**:
  - PRIMARY: Processes REQ/REL/HB, sends SYNC with majority consensus, tracks quorum health
  - BACKUP: Applies SYNC updates with term validation, redirects clients with NACK, monitors primary health
  - CANDIDATE: Buffers REQ messages, allows HB/REL, conducts Bully election with higher-ID peers
- **SYNC Consensus Protocol**:
  - After each state change, PRIMARY sends SYNC to all backups
  - Waits for SYNC_ACK from majority within 1-second timeout
  - Marks unresponsive backups as "suspect" but continues operation
  - Returns ACK count for validation (availability over strict consistency)
- **Bully Election Algorithm with Failover**:
  - **Failure Detection**: BACKUP monitors primary heartbeats (2.5-second timeout) or receives STEP_DOWN
  - **Election Trigger**: On primary failure, BACKUP → CANDIDATE, increment term
  - **Election Freeze**: CANDIDATE buffers incoming REQ messages, allows HB/REL to continue
  - **ELECTION Messages**: Send to all higher-ID coordinators with 2-second timeout
  - **OK Response**: Higher-ID nodes send OK and start their own election
  - **Election Victory**: No OK received → become PRIMARY via `_become_primary()`
  - **State Takeover**: New PRIMARY checks expired leases, processes pending REQs in FIFO order
  - **Active Lease Continuation**: Nodes continue working, update coordinator from COORDINATOR broadcasts
  - **Term Management**: All messages include term, reject STALE_TERM with NACK
  - **Concurrent Elections**: Higher term wins, equal term → higher ID wins
  - **Quorum Monitoring**: PRIMARY tracks majority COORD_HB_ACK responses, steps down if quorum lost 3+ seconds
  - **Split-Brain Prevention**: STEP_DOWN broadcasts, partition recovery via higher-term COORDINATOR messages

#### GUI Wrapper (`coordinator_gui.py`)
- **CoordinatorGUI Class**: Jupyter widgets interface wrapping core coordinator
- **Widget Management**: Dynamic node discovery with cached widget creation
- **Real-time Updates**: Separate thread for 10 FPS dashboard updates

#### Critical Sections in Code
1. **Client Message Processing Loop** (`_client_server()` method):
   - PRIMARY: Handles REQ (immediate grant/queue), REL (token transfer), HB (lease renewal)
   - BACKUP: Sends NACK redirect for all client messages
   - Automatic grant sending on lease expiry (PRIMARY only)
   - State synchronization to backups after each state change

2. **Coordinator Message Processing Loop** (`_coord_server()` method):
   - BACKUP: Receives SYNC messages and applies state snapshots, monitors primary failure
   - Sends SYNC_ACK acknowledgments to PRIMARY
   - Bully Election: Handles ELECTION/OK/COORDINATOR messages for leader election
   - Term validation: Rejects messages with stale terms using STALE_TERM NACK
   - Election timeout checking for CANDIDATE role

3. **Dashboard Update Loop** (`_update_dashboard()` method in GUI):
   - Real-time widget state updates
   - Lease countdown calculations
   - Dynamic widget creation/management

#### Shutdown Mechanism
- **Stop Button**: Triggers `_trigger_stop()` callback
- **Internal Signal**: Sends "INTERNAL_STOP" UDP message to wake up server thread
- **Socket Cleanup**: Explicit `socket.close()` in finally blocks
- **Thread Cleanup**: Daemon threads auto-terminate when main thread exits

### Node Implementations

#### Smart Node (`node.py`) Flow
1. **Random ID Generation**: `Node_{random.randint(10, 99)}`
2. **Work Simulation**: 2-4 second idle periods between requests
3. **Request Phase**: 
   - Send REQ message via ReliableSender
   - Wait for ACK (retry on timeout/NACK)
   - Wait for separate GRANT message
   - Update term from coordinator responses
4. **Critical Section Phase**: 
   - 3-12 second work simulation
   - Send HB every 2 seconds via ReliableSender
   - Handle ACK/NACK responses for heartbeats
5. **Release Phase**: 
   - Send REL via ReliableSender
   - Wait for ACK confirmation

#### Simple Node (`nodes/node*.py`) Flow
1. **Fixed ID**: Hardcoded as `Node_1`, `Node_2`, etc.
2. **Simplified Loop**: Sleep → REQ → Blocking recv → Sleep → REL → Repeat
3. **No Error Handling**: Vulnerable to network timeouts and coordinator failures

### Extension Points & Design Considerations

#### Scalability Limitations
- **Single Point of Failure**: Coordinator failure brings down entire system
- **UDP Reliability**: No message acknowledgment or retransmission
- **Memory Growth**: `known_nodes` set grows indefinitely
- **Threading Model**: Single-threaded message processing limits throughput

#### Potential Extensions
1. **Coordinator Replication**: Multiple coordinators with leader election
2. **Persistent State**: Store queue/lease state for coordinator restart
3. **Node Health Monitoring**: Detect and clean up failed nodes
4. **Message Reliability**: Add sequence numbers and acknowledgments
5. **Load Balancing**: Multiple critical sections with different coordinators
6. **Security**: Add authentication and message encryption
7. **Metrics**: Track access patterns, queue lengths, and lease utilization

#### Configuration Parameters
All hardcoded values that could be made configurable:
- `HOST = '0.0.0.0'` and `PORT = 50000` (coordinator binding)
- `COORD_IP = '192.168.1.101'` (node target)
- `LEASE_DURATION = 5.0` (lease timeout)
- Heartbeat interval (2.0 seconds)
- Socket timeout (2.0 seconds)
- GUI update rate (0.1 seconds)
- Work duration ranges (nodes)

## Dependencies

### Core Python Modules (Built-in)
- `socket`: UDP socket implementation for network communication
- `threading`: Thread management and synchronization (`threading.Lock()`)
- `time`: Timestamp handling and sleep operations
- `random`: Random ID generation and work duration simulation
- `collections`: `deque` for FIFO queue implementation
- `json`: Message serialization/deserialization for protocol layer
- `dataclasses`: Structured message type definitions
- `abc`: Abstract base classes for message protocol

### External Dependencies
- `ipywidgets`: Interactive GUI widgets for Jupyter dashboard
  - Used widgets: `VBox`, `GridBox`, `HTML`, `FloatProgress`, `Button`
  - Layout management and styling
- `IPython`: Jupyter notebook display functionality
  - `IPython.display.display()` for widget rendering

### Installation Commands
```bash
# Install using uv (recommended)
uv sync

# Or install manually with pip
pip install ipywidgets IPython jupyter

# Enable widget extensions (may be required)
jupyter nbextension enable --py widgetsnbextension
```

### Environment Requirements
- **Python Version**: 3.14+ (as specified in pyproject.toml)
- **Package Manager**: `uv` (recommended) or `pip`
- **Jupyter Notebook**: Required for GUI dashboard
- **Network Access**: UDP communication on port 50000
- **Operating System**: Cross-platform (tested on Linux/macOS/Windows)

## Design Philosophy & Trade-offs

### Simplicity Over Robustness
This implementation prioritizes educational clarity over production robustness:
- **Clear Protocol**: Simple text-based messages for easy debugging
- **Visual Feedback**: Real-time GUI shows system state transparently  
- **Minimal Dependencies**: Uses mostly built-in Python modules
- **Single-File Components**: Each node type in separate file for clarity

### Centralized vs Distributed Trade-offs
**Advantages of Centralized Approach:**
- Simple mutual exclusion logic (no complex distributed algorithms)
- Clear system state visualization
- Easy to reason about and debug
- FIFO fairness guaranteed
- Uses ephemeral port 50000 to avoid conflicts with well-known services

**Disadvantages:**
- Single point of failure
- Network bottleneck at coordinator
- No Byzantine fault tolerance
- Coordinator restart loses all state

### UDP vs TCP Choice
**Why UDP was chosen:**
- Lower latency for simple request/response pattern
- Simpler connection management (no connection state)
- More realistic for distributed systems (message-passing paradigm)
- Demonstrates handling of potential message loss

**Implications:**
- Messages can be lost (nodes handle with retries)
- No flow control or congestion management
- Simpler firewall traversal
- Requires application-level reliability mechanisms

## Code Quality & Style Notes

### Thread Safety Patterns
- **Single Lock Strategy**: One `state_lock` protects all shared state
- **Lock Ordering**: Always acquire lock before any state access
- **Atomic Operations**: State changes happen within single lock acquisition
- **Lock Scope**: Minimize time spent holding locks

### Error Handling Philosophy
- **Coordinator**: Robust error handling with graceful degradation
- **Smart Nodes**: Retry logic with exponential backoff for reliability
- **Simple Nodes**: Minimal error handling for educational simplicity
- **Logging**: Print statements for debugging (no formal logging framework)

### German Language Elements
The coordinator interface uses German text:
- `"Server Stoppen"` (Stop Server)
- `"ONLINE"/"GESTOPPT"` (Online/Stopped) 
- `"RUNNING"/"WAITING"/"IDLE"` state labels
- Consider internationalization for broader adoption