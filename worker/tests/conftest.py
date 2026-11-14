import os
import sys
from pathlib import Path

# Both services have a top-level main.py; make the worker copy win for these tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler",
)
os.environ.setdefault(
    "RABBITMQ_URL", "amqp://scheduler:scheduler_secret@localhost:5672"
)
