#Token bucket implementation of AATB
import asyncio
import logging
import threading
import time
import socket
import random
from asyncio import AbstractEventLoop
import report_data
import json
import sys
from dataclasses import asdict
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.WARNING, stream=sys.stdout)

class AdaptiveTokenBucket:
    """Adaptive Token Bucket for AATB."""
    def __init__(self, size: float, tokens: float, initial_rate: float,
                 start_time: float, id: int, step_one: float, step_two: float,
                 controller_addr, report_window=10):
        """
        Initialize the adaptive token bucket.
        initial_rate is per minute; it is converted to per second.
        """
        self.tokens = tokens
        self.bucket_size = size
        self.initial_rate = initial_rate  # rate per minute
        self.rate = self.convert_rate(initial_rate)  # per second
        self.start_time = start_time
        self.last_used = start_time
        self.id = id
        self.step_one = step_one
        self.step_two = step_two
        self.controller_addr = controller_addr
        self.report_window = report_window
        self.update_counter = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(3)
        self.lock_sent = threading.Lock()
        self.lock_rate = threading.Lock()
        self.r_current_window: report_data.ReceivedData = report_data.ReceivedData(0, 0, 0, 15)
        self.last_rate_change = 0
        self.last_reported_congestion = 0
        self.stats = report_data.Stats(id=self.id, type='stat', sent=0)
        self.archived_sent = 0
        self.sched = BackgroundScheduler(daemon=True)
        self.sched.add_job(self.s_routine_update, 'interval', seconds=self.report_window, id='routine_updates')
        self.sched.start()
        self.acquire_lock = threading.Lock()
        self.next_acquire = 0

    def shall_update(self) -> bool:
        """Check whether an update should be performed."""
        now = round(time.time(), 2)
        if (now - self.last_reported_congestion) < self.report_window:
            logging.warning(f"Client{self.id} shouldn't update, now:{round(now - self.start_time, 2)}, "
                            f"last_change:{round(self.last_rate_change - self.start_time, 2)}, "
                            f"last congestion:{round(self.last_reported_congestion - self.start_time, 2)}")
            return False
        return True

    def s_routine_update(self):
        """Routine update: send stats to controller and update parameters."""
        if time.time() - self.last_reported_congestion >= self.report_window:
            with self.lock_sent:
                data = json.dumps(asdict(self.stats))
                self.archived_sent = self.stats.sent
                self.stats.sent = 0
                self.update_counter += 1
            r = None
            try:
                self.socket.sendto(data.encode(), self.controller_addr)
                r = self.socket.recv(1024)
            except socket.timeout:
                logging.warning(f"Client{self.id} timeout while waiting for response")
            except Exception as e:
                logging.error(f"Failed to send data to {self.controller_addr} with error: {e}")
            if r:
                r = r.decode()
                logging.warning(f"Client{self.id} received data: {r}")
                self.r_current_window = report_data.ReceivedData(**json.loads(r))
                self.update_params()

    async def congestion_notification(self, loop: AbstractEventLoop):
        """Notify controller of congestion and update parameters accordingly."""
        self.last_reported_congestion = round(time.time(), 2)
        with self.lock_sent:
            data = asdict(self.stats)
            self.archived_sent = self.stats.sent
            self.stats.sent = 0
            self.update_counter += 1
        data['type'] = 'cong'
        data = json.dumps(data)
        r = None
        try:
            await loop.sock_sendto(self.socket, data.encode(), self.controller_addr)
            r = await asyncio.wait_for(loop.sock_recv(self.socket, 1024), 3)
        except socket.timeout:
            logging.warning(f"Client{self.id} timeout while waiting for response")
        except Exception as e:
            logging.error(f"Failed to send data to {self.controller_addr} with error: {e}")
        if r:
            r = r.decode()
            self.r_current_window = report_data.ReceivedData(**json.loads(r))
            logging.warning(f"Client{self.id} received data: {r} as congestion reply")
        await self.update_congestion()

    def update_params(self):
        """Update the token bucket parameters based on received data."""
        min_increase = 0.01
        now = round(time.time(), 2)
        if self.r_current_window.reported_429 > 0:
            self.network_congestion()
            return
        elif now - self.last_rate_change >= self.report_window:
            active = 1.0 if self.r_current_window.active_clients == 0 else self.r_current_window.active_clients
            avg_load: float = self.r_current_window.total_requests / active
            logging.warning(f"past sent:{self.archived_sent}, average load: {avg_load}, active: {active}")
            with self.lock_sent:
                ts = self.archived_sent + self.stats.sent
            if (ts < (0.75 * avg_load)) or (active == 1) or (avg_load == 0):
                new_rate = round(max(self.rate * self.step_one, self.rate + min_increase), 2)
                logging.warning(f"client{self.id} increase rate at {self.step_one}x to {new_rate}")
                self.increase(new_rate)
            else:
                new_rate = round(max(self.rate * self.step_two, self.rate + min_increase), 2)
                logging.warning(f"client{self.id} increase rate at {self.step_two}x to {new_rate}")
                self.increase(new_rate)
        else:
            logging.warning(f"Client{self.id} doesnt update last change:{round(now - self.last_rate_change, 2)}")

    async def update_congestion(self):
        """Update parameters when congestion is detected."""
        active = 1 if self.r_current_window.active_clients == 0 else self.r_current_window.active_clients
        avg_requests = self.r_current_window.total_requests / active
        total_reported_429 = self.r_current_window.reported_429 + 1
        token_rate = self.r_current_window.token_rate
        wait_until = round((total_reported_429 * 60 / token_rate) + random.uniform(0, 1), 2)
        logging.warning(f"client{self.id} predicted wait time at {round(time.time() - self.start_time)}: {wait_until}")
        with self.lock_sent:
            ts = self.archived_sent + self.stats.sent
        with self.lock_rate:
            rate = self.rate
        if ts < 0.5 * avg_requests:
            new_rate = max(0.01, round(rate / 2.0, 2))
        else:
            new_rate = max(0.01, round(rate / 3.0, 2))
        logging.warning(f"Client{self.id} reducing rate due to 429 to {new_rate}")
        now = round(time.time(), 2)
        logging.warning(f"Client{self.id} decreasing rate from {self.rate} to {new_rate} at {round(now - self.start_time, 2)}")
        with self.lock_rate:
            self.tokens = 1.1
            self.rate = new_rate
            self.last_rate_change = now
        with self.acquire_lock:
            self.next_acquire = wait_until + round(time.time(), 2)
        logging.warning(f"client{self.id} next acquired lock {round(self.next_acquire - now, 2)}")

    def convert_rate(self, rate: float):
        """Convert a rate from per minute to per second."""
        return max(round(rate / 60, 2), 0.01)

    async def acquire(self):
        """Acquire a token; wait if necessary based on token availability and next acquire time."""
        now = time.time()
        sleep_time = 0
        token_sleep = False
        with self.acquire_lock:
            acquire_time = self.next_acquire
            self.next_acquire = 0
        with self.lock_rate:
            generated_tokens = ((time.time() - self.last_used) * self.rate)
            self.tokens = min(self.bucket_size, self.tokens + generated_tokens)
            logging.warning(f"Client{self.id} tokens {self.tokens} at {round(now - self.start_time, 2)}")
            if self.tokens >= 1.0:
                if now > acquire_time:
                    self.tokens -= 1
                    self.last_used = round(time.time(), 2)
                    return True
                else:
                    sleep_time = round(acquire_time - now, 2)
                    logging.warning(f"tokens:{self.tokens} but need to sleep for {sleep_time}")
                    token_sleep = False
            else:
                sleep_time = max(round((1 - self.tokens) / self.rate, 2), round(acquire_time - now, 2))
                token_sleep = True
                logging.warning(f"tokens:{self.tokens} not enough and need to sleep for {sleep_time}")
        logging.warning(f"client{self.id} need to sleep for {sleep_time}")
        await asyncio.sleep(sleep_time)
        with self.lock_rate:
            self.tokens = max(self.tokens - 1, 0)
        self.last_used = round(time.time(), 2)
        return True

    def increase(self, new_rate):
        """Increase the rate to new_rate and update last rate change time."""
        with self.lock_rate:
            now = round(time.time(), 2)
            logging.warning(f"Client{self.id} increasing rate from {self.rate} to {new_rate} at {round(now - self.start_time, 2)}")
            self.rate = new_rate
            self.last_rate_change = now

    def stop(self):
        """Stop the token bucket by closing socket and shutting down scheduler."""
        try:
            self.socket.close()
            self.sched.remove_job('routine_updates')
            self.sched.shutdown()
        except Exception as e:
            logging.error(f"Failed to delete instance: {str(e)}")

    def _increase_sent(self):
        """Increment the count of sent requests."""
        with self.lock_sent:
            self.stats.sent += 1

    def network_congestion(self):
        """Handle network congestion by backing off."""
        backoff = round(self.report_window + random.uniform(-2, 2), 2)
        with self.acquire_lock:
            self.next_acquire = backoff + time.time()
        logging.warning(f"Client{self.id} backing off due to system congestion for {backoff}s")
