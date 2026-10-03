# Central place for process-scoped shared caches used by schedulers and tests
EXPIRIES_CACHE: dict[tuple[str, str, str], list] = {}
