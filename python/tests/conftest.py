import os

# unit tests never write run history to a ClickHouse that may or may not be running
os.environ.setdefault("REALY_RUN_HISTORY", "0")
