#!/usr/bin/env python3
import subprocess
import time
import sys
import signal
import argparse
from typing import Optional, List
from dataclasses import dataclass
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.console import Console
from rich.layout import Layout
import threading
import time
from typing import Dict
import re

NODE_RE = re.compile(r"\[(Node_\d+)\]")
COORD_RE = re.compile(r"\[(Primary|Backup_\d+|Coordinator_\d+)\]")
CONTAINER_RE = re.compile(r"^(cs_[a-zA-Z0-9_]+)\s+\|")



class DemoClusterDashboard:
	"""
    Live dashboard for the demo cluster.
    Displays nodes, coordinators, roles, and general system status.
    """

	STALE_TIMEOUT = 15  # seconds before removing inactive entries

	def __init__(self, refresh_hz: float = 2.0):
		self.console = Console(force_terminal=True)
		self.refresh_interval = 1.0 / refresh_hz
		self.running = True
		self.thread = threading.Thread(target=self._run, daemon=True)

		# Internal authoritative state
		self.state: Dict[str, Dict] = {
			"nodes": {},          # id -> {role, status, last_update}
			"coordinators": {},   # id -> {role, status, last_update}
			"system_status": "Initializing..."
		}

	# ---------- lifecycle ----------

	def start(self):
		self.thread.start()

	def stop(self):
		self.running = False

	# ---------- helpers ----------

	def _norm_id(self, raw: str) -> str:
		"""Normalize IDs to prevent duplicates"""
		return raw.strip().replace(" ", "").upper()

	def _cleanup_stale(self):
		now = time.time()

		for bucket in ("nodes", "coordinators"):
			for key, info in list(self.state[bucket].items()):
				if now - info.get("last_update", now) > self.STALE_TIMEOUT:
					del self.state[bucket][key]

	# ---------- public API ----------

	def update_state(
		self,
		nodes: Dict[str, Dict],
		coordinators: Dict[str, Dict],
		system_status: str
	):
		now = time.time()

		for nid, info in nodes.items():
			nid = self._norm_id(nid)
			self.state["nodes"][nid] = {
				"role": info.get("role", "-"),
				"status": info.get("status", "-"),
				"last_update": now
			}

		for cid, info in coordinators.items():
			cid = self._norm_id(cid)
			self.state["coordinators"][cid] = {
				"role": info.get("role", "-"),
				"status": info.get("status", "-"),
				"last_update": now
			}

		self.state["system_status"] = system_status
		self._cleanup_stale()

	# ---------- rendering ----------

	def _build_layout(self):
		layout = Layout()
		layout.split_column(
			Layout(name="header", size=3),
			Layout(name="body"),
			Layout(name="footer", size=3),
		)
		layout["body"].split_row(
			Layout(name="left"),
			Layout(name="right")
		)
		return layout

	def _render_header(self):
		text = (
			f"[bold blue]Demo Cluster Dashboard[/bold blue] | "
			f"Status: [yellow]{self.state['system_status']}[/yellow]"
		)
		return Panel(text, style="bold white on dark_green")

	def _render_nodes_table(self):
		table = Table(title="Nodes", expand=True)
		table.add_column("Node")
		table.add_column("Role")
		table.add_column("Status")

		for nid, info in sorted(self.state["nodes"].items()):
			status_color = {
				"RUNNING": "green",
				"WAITING": "yellow",
				"FAILED": "red",
			}.get(info.get("status"), "white")

			table.add_row(
				nid,
				info.get("role", "-"),
				f"[{status_color}]{info.get('status', '-')}[/{status_color}]"
			)

		if not self.state["nodes"]:
			table.add_row("-", "-", "-")

		return Panel(table)

	def _render_coordinators_table(self):
		table = Table(title="Coordinators", expand=True)
		table.add_column("Coordinator")
		table.add_column("Role")
		table.add_column("Status")

		for cid, info in sorted(self.state["coordinators"].items()):
			status_color = {
				"PRIMARY": "green",
				"BACKUP": "cyan",
				"CANDIDATE": "yellow",
				"FAILED": "red",
			}.get(info.get("role"), "white")

			table.add_row(
				cid,
				info.get("role", "-"),
				f"[{status_color}]{info.get('status', '-')}[/{status_color}]"
			)

		if not self.state["coordinators"]:
			table.add_row("-", "-", "-")

		return Panel(table)

	def _render_footer(self):
		return Panel(
			f"Nodes: {len(self.state['nodes'])} | "
			f"Coordinators: {len(self.state['coordinators'])}"
		)

	def _run(self):
		layout = self._build_layout()
		with Live(layout, console=self.console, refresh_per_second=10):
			while self.running:
				layout["header"].update(self._render_header())
				layout["left"].update(self._render_nodes_table())
				layout["right"].update(self._render_coordinators_table())
				layout["footer"].update(self._render_footer())
				time.sleep(self.refresh_interval)

		self.console.print("[bold red]Demo Dashboard stopped[/bold red]")


@dataclass
class DemoResult:
	scenario: str
	success: bool
	duration: float
	details: str

class DockerDemoRunner:
	def __init__(self, compose_file: str = "docker-compose.yml", dashboard: Optional[DemoClusterDashboard] = None):
		self.compose_file = compose_file
		self.process: Optional[subprocess.Popen] = None
		self.dashboard = dashboard

	def docker_compose_cmd(self, *args: str) -> List[str]:
		return ["docker-compose", "-f", self.compose_file] + list(args)

	def start_cluster(self) -> bool:
		print("\n=== Starting cluster ===")
		try:
			subprocess.run(
				self.docker_compose_cmd("up", "-d", "--build"),
				check=True,
				capture_output=True
			)
			print("✓ Cluster started successfully")
			return True
		except subprocess.CalledProcessError as e:
			print(f"✗ Failed to start cluster: {e.stderr.decode()}")
			return False

	def stop_cluster(self) -> None:
		print("\n=== Stopping cluster ===")
		subprocess.run(
			self.docker_compose_cmd("down"),
			capture_output=True
		)
		print("✓ Cluster stopped")

	def follow_logs(self, duration: int, services: Optional[List[str]] = None) -> None:
		cmd = self.docker_compose_cmd("logs", "-f", "--tail=20")
		if services:
			cmd.extend(services)

		self.process = subprocess.Popen(
			cmd,
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True
		)

		start_time = time.time()
		try:
			while time.time() - start_time < duration:
				line = self.process.stdout.readline()
				if line:
					print(line.rstrip())
					if self.dashboard:
						nodes_update = {}
						coords_update = {}
						system_status = "Regular traffic"

						# --- Node updates ---
						if "REQ" in line:
							m = NODE_RE.search(line)
							if m:
								node = m.group(1)
								nodes_update[node] = {"role": "CLIENT", "status": "WAITING"}

						elif "GRANT" in line:
							m = NODE_RE.search(line)
							if m:
								node = m.group(1)
								nodes_update[node] = {"role": "CLIENT", "status": "RUNNING"}

						# --- Coordinator updates ---
						elif "ELECTION" in line:
							m = CONTAINER_RE.search(line)
							if m:
								coord = m.group(1)
								coords_update[coord] = {"role": "CANDIDATE", "status": "RUNNING"}
								system_status = "Running election"

						elif "COORDINATOR" in line or "LEADER" in line or "PRIMARY" in line:
							m = CONTAINER_RE.search(line)
							if m:
								new_primary = m.group(1)
								# Assign PRIMARY to this coordinator
								coords_update[new_primary] = {"role": "PRIMARY", "status": "RUNNING"}

								# Make all other known coordinators BACKUP
								for existing_coord in self.dashboard.state["coordinators"]:
									if existing_coord != new_primary:
										# Only overwrite if not a candidate
										current_role = self.dashboard.state["coordinators"][existing_coord].get("role")
										if current_role != "CANDIDATE":
											coords_update[existing_coord] = {"role": "BACKUP",
																			 "status":
																				 self.dashboard.state["coordinators"][
																					 existing_coord].get("status",
																										 "RUNNING")}
								system_status = "Regular traffic"

						# --- Update dashboard ---
						if nodes_update or coords_update:
							self.dashboard.update_state(
								nodes=nodes_update,
								coordinators=coords_update,
								system_status=system_status
							)

				else:
					break
		except KeyboardInterrupt:
			pass
		finally:
			if self.process:
				self.process.terminate()
				self.process.wait()

	def stop_container(self, container: str) -> None:
		print(f"\n=== Stopping {container} ===")
		subprocess.run(
			self.docker_compose_cmd("stop", container),
			capture_output=True
		)
		print(f"✓ {container} stopped")

	def run_scenario_critical_section(self) -> DemoResult:
		print("\n" + "="*60)
		print("SCENARIO 1: Critical Section Access")
		print("="*60)
		print("\nThis scenario demonstrates:")
		print("- Nodes requesting critical section access")
		print("- Coordinator granting exclusive access")
		print("- Nodes holding and releasing the critical section")
		print("\nObserve the REQ → GRANT → work → REL cycle")
		print("\n" + "="*60)

		if not self.start_cluster():
			return DemoResult("critical_section", False, 0.0, "Failed to start cluster")

		if self.dashboard:
			self.dashboard.update_state(
				nodes={},
				coordinators={
					"Coordinator_1": {"role": "PRIMARY", "status": "RUNNING"}
				},
				system_status="Regular traffic"
			)

		print("\nWaiting 5 seconds for cluster initialization...")
		time.sleep(5)

		print("\n--- Monitoring nodes (30 seconds) ---")
		start_time = time.time()
		self.follow_logs(30, ["node1", "node2", "node3"])
		duration = time.time() - start_time

		self.stop_cluster()

		return DemoResult(
			"critical_section",
			True,
			duration,
			"Critical section access demonstrated successfully"
		)

	def run_scenario_leader_election(self) -> DemoResult:
		print("\n" + "="*60)
		print("SCENARIO 2: Leader Election and Failover")
		print("="*60)
		print("\nThis scenario demonstrates:")
		print("- Normal operation with primary coordinator")
		print("- Primary failure detection")
		print("- Bully algorithm election among backups")
		print("- Automatic failover to new coordinator")
		print("\n" + "="*60)

		if not self.start_cluster():
			return DemoResult("leader_election", False, 0.0, "Failed to start cluster")

		print("\nWaiting 10 seconds for normal operation...")
		time.sleep(10)

		print("\n--- Initial cluster state ---")
		self.follow_logs(5)

		print("\n" + "!"*60)
		print("MANUAL ACTION REQUIRED:")
		print("Kill the primary coordinator now by:")
		print("  1. Pressing Ctrl+C on the primary laptop, OR")
		print("  2. Running: docker-compose stop primary")
		print("\nThe demo will continue automatically after 30 seconds...")
		print("!"*60)

		input("\nPress ENTER when you have killed the primary coordinator...")

		print("\n--- Observing election and failover (30 seconds) ---")
		print("Watch for:")
		print("- ELECTION messages from backup coordinators")
		print("- OK responses from higher-ID backups")
		print("- COORDINATOR broadcast from new leader")
		print("- Nodes reconnecting to new coordinator")
		print()

		start_time = time.time()
		self.follow_logs(30)
		duration = time.time() - start_time

		self.stop_cluster()

		return DemoResult(
			"leader_election",
			True,
			duration,
			"Leader election and failover demonstrated successfully"
		)

	def run_scenario_all(self) -> List[DemoResult]:
		results = []

		dashboard =DemoClusterDashboard()
		dashboard.start()
		self.dashboard = dashboard

		result1 = self.run_scenario_critical_section()
		results.append(result1)

		dashboard.stop()

		print("\n\n" + "="*60)
		print("Waiting 5 seconds before next scenario...")
		print("="*60)
		time.sleep(5)

		dashboard = DemoClusterDashboard()
		dashboard.start()
		self.dashboard = dashboard

		result2 = self.run_scenario_leader_election()
		results.append(result2)

		dashboard.stop()


		return results

	def print_summary(self, results: List[DemoResult]) -> None:
		print("\n\n" + "="*60)
		print("DEMO SUMMARY")
		print("="*60)
		for result in results:
			status = "✓ PASS" if result.success else "✗ FAIL"
			print(f"\n{status} - {result.scenario}")
			print(f"  Duration: {result.duration:.1f}s")
			print(f"  Details: {result.details}")
		print("\n" + "="*60)

def main():
	parser = argparse.ArgumentParser(
		description='Docker demo runner for distributed critical section system'
	)
	parser.add_argument(
		'--scenario',
		choices=['critical', 'election', 'all'],
		default='all',
		help='Scenario to run (default: all)'
	)
	parser.add_argument(
		'--compose-file',
		default='docker-compose.yml',
		help='Path to docker-compose file'
	)
	args = parser.parse_args()

	runner = DockerDemoRunner(args.compose_file)

	def cleanup_handler(signum, frame):
		print("\n\nInterrupted! Cleaning up...")
		runner.stop_cluster()
		sys.exit(0)

	signal.signal(signal.SIGINT, cleanup_handler)
	signal.signal(signal.SIGTERM, cleanup_handler)



	try:
		results = []

		if args.scenario == 'critical':
			result = runner.run_scenario_critical_section()
			results.append(result)
		elif args.scenario == 'election':
			result = runner.run_scenario_leader_election()
			results.append(result)
		else:
			results = runner.run_scenario_all()

		runner.print_summary(results)

	except Exception as e:
		print(f"\n✗ Demo failed with error: {e}")
		runner.stop_cluster()
		sys.exit(1)

if __name__ == "__main__":
	main()
