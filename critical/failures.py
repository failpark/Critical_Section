import socket
import random
import time
from typing import Tuple, Dict


class MessageDropWrapper:
	"""Wraps a socket to probabilistically drop outgoing messages."""

	def __init__(self, original_socket: socket.socket, drop_rate: float = 0.0):
		self.original_socket = original_socket
		self.drop_rate = drop_rate
		self.dropped_count = 0
		self.sent_count = 0

	def __getattr__(self, name):
		"""Delegate all other socket methods to the original socket."""
		return getattr(self.original_socket, name)

	def sendto(self, data: bytes, address: Tuple[str, int]) -> int:
		"""Intercept sendto and probabilistically drop messages."""
		self.sent_count += 1
		if random.random() < self.drop_rate:
			self.dropped_count += 1
			return len(data)  # Pretend it was sent
		return self.original_socket.sendto(data, address)

	def get_stats(self) -> Dict[str, int]:
		"""Return message drop statistics."""
		return {
			'sent': self.sent_count,
			'dropped': self.dropped_count,
			'delivered': self.sent_count - self.dropped_count
		}


class NodeFailureSimulator:
	"""Simulates fail-stop-return failures for nodes.

	Configuration:
	- Recovery time: 5-15 seconds (random)
	- Permanent failure probability: 10%
	- Target: 90% probability of at least one failure in 30 seconds across all nodes
	"""

	RECOVERY_TIME_MIN = 5.0  # seconds
	RECOVERY_TIME_MAX = 15.0  # seconds
	PERMANENT_FAILURE_PROB = 0.10  # 10% chance of permanent failure

	def __init__(self, failure_prob: float = 0.0, permanent_prob: float = PERMANENT_FAILURE_PROB, node_id: str = "Unknown"):
		"""
		Initialize the failure simulator.

		Args:
			failure_prob: Probability of failure per check (e.g., 0.015 for ~1.5% per second)
			permanent_prob: Probability that a failure is permanent (default 10%)
			node_id: Node identifier for logging
		"""
		self.failure_prob = failure_prob
		self.permanent_prob = permanent_prob
		self.node_id = node_id

		self.is_currently_failed = False
		self.is_permanent_failure = False
		self.failure_start_time = None
		self.recovery_time = None
		self.total_failures = 0

	def check_for_failure(self) -> bool:
		"""
		Check if a failure should be triggered.
		Call this periodically (e.g., every iteration of main loop).

		Returns:
			True if failure was triggered, False otherwise
		"""
		if self.is_currently_failed:
			return False  # Already failed

		if random.random() < self.failure_prob:
			# Trigger failure
			self.is_currently_failed = True
			self.failure_start_time = time.time()
			self.total_failures += 1

			# Determine if permanent
			self.is_permanent_failure = random.random() < self.permanent_prob

			if self.is_permanent_failure:
				self.recovery_time = None
				print(f"[{self.node_id}] *** PERMANENT FAILURE TRIGGERED ***")
			else:
				self.recovery_time = random.uniform(self.RECOVERY_TIME_MIN, self.RECOVERY_TIME_MAX)
				print(f"[{self.node_id}] *** FAILURE TRIGGERED - Will recover in {self.recovery_time:.1f}s ***")

			return True

		return False

	def is_failed(self) -> bool:
		"""Check if currently in failed state."""
		return self.is_currently_failed

	def maybe_recover(self) -> bool:
		"""
		Check if it's time to recover from failure.
		Call this while in failed state.

		Returns:
			True if recovered, False if still failed
		"""
		if not self.is_currently_failed:
			return True  # Not failed

		if self.is_permanent_failure:
			return False  # Never recover

		if self.failure_start_time and self.recovery_time:
			elapsed = time.time() - self.failure_start_time
			if elapsed >= self.recovery_time:
				# Recover
				self.is_currently_failed = False
				self.failure_start_time = None
				self.recovery_time = None
				print(f"[{self.node_id}] *** RECOVERED FROM FAILURE after {elapsed:.1f}s ***")
				return True

		return False

	def get_stats(self) -> Dict[str, any]:
		"""Return failure statistics."""
		return {
			'total_failures': self.total_failures,
			'currently_failed': self.is_currently_failed,
			'is_permanent': self.is_permanent_failure,
			'time_in_failure': time.time() - self.failure_start_time if self.failure_start_time else 0
		}
