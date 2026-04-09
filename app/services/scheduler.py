import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from app.config import APP_TIMEZONE
from app.repository import get_settings


class DailyScheduler:
    def __init__(self, run_callback):
        self.run_callback = run_callback
        self.thread = None
        self.last_run_key = None
        self._stop_event = threading.Event()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._loop, daemon=True, name="aeco-daily-scheduler")
        self.thread.start()

    def stop(self):
        self._stop_event.set()

    def _loop(self):
        timezone = ZoneInfo(APP_TIMEZONE)
        while not self._stop_event.is_set():
            settings = get_settings()
            now = datetime.now(timezone)
            try:
                hour, minute = (settings["run_time"] or "08:00").split(":")
                target_hour = int(hour)
                target_minute = int(minute)
            except ValueError:
                target_hour = 8
                target_minute = 0

            run_key = f"{now.date().isoformat()}-{target_hour:02d}:{target_minute:02d}"
            should_run = (
                now.hour > target_hour or (now.hour == target_hour and now.minute >= target_minute)
            ) and self.last_run_key != run_key

            if should_run:
                self.run_callback(trigger="scheduled")
                self.last_run_key = run_key

            time.sleep(30)
