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
- **Commands**: User runs external commands (e.g., `cd critical && python3 coordinator.py`)

## Architecture

### Core Components

1. **Coordinator System**: 
   - **Core Logic** (`critical/coordinator.py`): Enhanced with full protocol layer integration
   - **GUI Interface** (`critical/coordinator_gui.py`): Jupyter widget-based dashboard wrapper  
   - **Notebook Interface** (`critical/coordinator.ipynb`): Interactive Jupyter notebook demo
   - Protocol-aware message handling with ACK/NACK responses
   - Duplicate detection using per-node sequence number tracking
   - Term-based consensus support for future leader election
   - Thread-safe implementation with `threading.Lock()` for state synchronization
   - Binds to `0.0.0.0:5000` (all interfaces) for maximum connectivity

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
- **queue**: FIFO queue of (node_id, address) pairs awaiting access
- **known_nodes**: Set of all nodes that have ever contacted coordinator
- **term**: Current consensus term for leader election compatibility
- **last_seen_seq**: Dict mapping node_id to last processed sequence number

### Network Configuration & Technical Details

#### Socket Configuration
- **Coordinator**: UDP socket bound to `0.0.0.0:5000`
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

1. **Start Coordinator**:
   ```bash
   # Option 1: Standalone coordinator (no GUI, command line only)
   python critical/coordinator.py
   
   # Option 2: Jupyter notebook with interactive GUI dashboard
   jupyter notebook critical/coordinator.ipynb
   # Execute all cells in sequence to start the coordinator server
   # Dashboard will appear inline with real-time node status
   
   # Option 3: Use the GUI module programmatically
   python -c "from critical.coordinator_gui import start_coordinator_gui; start_coordinator_gui()"
   ```

2. **Start Nodes**:
   ```bash
   # Simple nodes (basic REQ/REL only)
   python critical/nodes/node1.py
   python critical/nodes/node2.py
   python critical/nodes/node3.py
   python critical/nodes/node4.py
   python critical/nodes/node5.py
   
   # Smart node (with heartbeat and error handling)
   python critical/node.py
   ```

### Testing Strategy

No formal test framework is configured. Manual testing approach:
- **Mutual Exclusion**: Run multiple nodes simultaneously, verify only one in critical section
- **FIFO Ordering**: Check dashboard queue positions match request order
- **Lease Expiry**: Let smart nodes hold tokens and observe automatic expiry after 5 seconds
- **Heartbeat Functionality**: Verify smart nodes maintain leases longer than 5 seconds
- **Network Resilience**: Test node disconnection/reconnection scenarios

## Technical Implementation Details

### Coordinator System Architecture

#### Core Coordinator (`coordinator.py`)
- **Coordinator Class**: Reusable coordinator implementation with clean separation of concerns
- **State Variables** (Protected by `state_lock`):
  - `running`: Global shutdown flag for clean termination
  - `queue`: `collections.deque` storing `(node_id, address)` tuples
  - `current_holder`: String ID of token holder or `None`
  - `lease_expiry`: Float timestamp when current lease expires
  - `known_nodes`: Set of all discovered node IDs for GUI management

#### GUI Wrapper (`coordinator_gui.py`)
- **CoordinatorGUI Class**: Jupyter widgets interface wrapping core coordinator
- **Widget Management**: Dynamic node discovery with cached widget creation
- **Real-time Updates**: Separate thread for 10 FPS dashboard updates

#### Critical Sections in Code
1. **Message Processing Loop** (`_udp_server()` method):
   - Handles REQ: Immediate grant or queue insertion
   - Handles REL: Token transfer to next queued node
   - Handles HB: Lease renewal for current holder
   - Automatic grant sending on lease expiry

2. **Dashboard Update Loop** (`_update_dashboard()` method in GUI):
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
- `HOST = '0.0.0.0'` and `PORT = 5000` (coordinator binding)
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
- **Network Access**: UDP communication on port 5000
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