_default:
	@just --list

start:
	uv run critical/coordinator.py

node:
	uv run critical/node.py
