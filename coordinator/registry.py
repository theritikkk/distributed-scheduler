"""
Worker registry: track worker heartbeats and mark stale workers offline.
"""
import logging
import threading
import time

logger = logging.getLogger(__name__)


class Registry:

    def __init__(self, db_conn_factory):
        self._get_conn = db_conn_factory
        self._lock = threading.RLock()

    def run_heartbeat_checker( self, interval_sec = 30, stale_minutes = 2 ):
        """Background thread: mark workers offline if no heartbeat for 2 minutes."""

        while True:
            try:
                time.sleep( interval_sec )
                conn = self._get_conn()

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE workers SET status = 'offline'
                            WHERE last_heartbeat < NOW() - INTERVAL '2 minutes' AND status != 'offline'
                            """
                        )
                        if cur.rowcount and cur.rowcount > 0:
                            logger.info( "marked %s worker(s) offline", cur.rowcount )
                finally:
                    conn.close()
            except Exception as e:
                logger.exception( "heartbeat checker: %s", e )
