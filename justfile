_default:
	@just --list

# Start coordinator
start:
	uv run critical/coordinator.py

# Start smart node
node:
	uv run critical/node.py

# Start PRIMARY coordinator
primary:
	uv run critical/coordinator.py --role primary --id 100 --peers 127.0.0.1:50002

# Start BACKUP coordinator
backup:
	uv run critical/coordinator.py --role backup --id 98 --peers 127.0.0.1:50000 --client-port 50002 --coord-port 50003

