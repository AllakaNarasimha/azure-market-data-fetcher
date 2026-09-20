"""Centralized market time constants for the project.

Provide a simple `MarketTimes` class with static accessors while keeping
module-level constants for backwards compatibility.
"""
from datetime import time


class MarketTimes:
	"""Encapsulates market timezone and trading hours.

	Use the static accessors when you prefer explicit names, e.g.
	`MarketTimes.timezone()` or `MarketTimes.open()`.
	"""

	TIMEZONE = "Asia/Kolkata"
	OPEN = time(9, 15)
	CLOSE = time(16, 0)

	@staticmethod
	def timezone() -> str:
		return MarketTimes.TIMEZONE

	@staticmethod
	def open() -> time:
		return MarketTimes.OPEN

	@staticmethod
	def close() -> time:
		return MarketTimes.CLOSE