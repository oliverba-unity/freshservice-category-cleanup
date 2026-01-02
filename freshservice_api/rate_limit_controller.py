import threading
import time
from typing import Mapping, Any

class RateLimitController:
    def __init__(self, headroom: int = 10):
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

        self.headroom = headroom

        self.server_ratelimit_remaining = 160
        self.server_ratelimit_total = 160

        self.requests_in_flight = 0
        self.last_request_timestamp = 0.0
        self.retry_after_timestamp = 0.0

        self.probe_wait_seconds = 1.0
        self.probe_scheduled = False

    def block_until_ready(self):
        """
        Temporarily blocks the calling thread for a safe interval, then registers it as 'in-flight'.
        """
        with self._condition:
            while True:
                now = time.time()

                if now < self.retry_after_timestamp:
                    self._condition.wait(timeout=self.retry_after_timestamp - now)
                    continue

                base_interval_seconds = 60.0 / self.server_ratelimit_total

                ratelimit_remaining = self.server_ratelimit_remaining - self.requests_in_flight

                if ratelimit_remaining > self.headroom:

                    self.probe_wait_seconds = max(1.0, base_interval_seconds)

                    braking_threshold = self.headroom * 3

                    if ratelimit_remaining > braking_threshold:
                        interval_multiplier = 1.0
                    else:
                        interval_multiplier = braking_threshold / max(1, ratelimit_remaining)

                    required_interval_seconds = base_interval_seconds * interval_multiplier
                    seconds_since_last_request = now - self.last_request_timestamp

                    if seconds_since_last_request < required_interval_seconds:
                        seconds_to_wait = required_interval_seconds - seconds_since_last_request
                        self._condition.wait(timeout=seconds_to_wait)
                        continue
                    else:
                        self.requests_in_flight += 1
                        self.last_request_timestamp = now

                        # Allow the caller to place a request
                        return

                else:
                    """
                    The number of requests remaining before we hit the ratelimit is less than the headroom.
                    
                    To reduce the chance that we (or any other API consumers) hit the rate limit:
                                        
                    Make this thread (and request) wait if there are already other requests in flight, or if another
                    thread has scheduled a "probe".
                      
                    Otherwise, designate this thread as a "probe" which:
                     1. Waits for {probe_wait_seconds}
                     2. Re-checks if we are still using our headroom 
                     3. Allows the caller to make a request (which updates {server_ratelimit_remaining})
                     4. Increases {probe_wait_seconds}, so if another thread finds we are still using our headroom, the 
                        next probe waits longer
                    """

                    if self.requests_in_flight > 0 or self.probe_scheduled:
                        print(f"Remaining quota ({ratelimit_remaining}) below headroom ({self.headroom}). "
                              f"Sleeping until woken.")
                        self._condition.wait()
                        continue

                    else:
                        # Make other threads aware that this thread will be the "probe" and allow one request to execute
                        self.probe_scheduled = True
                        print(f"Remaining quota ({ratelimit_remaining}) below headroom ({self.headroom}). "
                              f"Sleeping for {self.probe_wait_seconds}s...")

                        try:
                            self._condition.wait(timeout=self.probe_wait_seconds)
                        finally:
                            self.probe_scheduled = False

                        # After waiting, check if we still don't have enough remaining ratelimit
                        if (self.server_ratelimit_remaining - self.requests_in_flight) <= self.headroom:
                            # Increase backoff for the next probe, up to a maximum of 60 seconds
                            self.probe_wait_seconds = min(self.probe_wait_seconds * 2, 60.0)

                            self.requests_in_flight += 1
                            self.last_request_timestamp = time.time()

                            # Allow the caller to place a request
                            return

                        continue

    def update_and_notify(self, headers: Mapping[str, Any] = None):
        """
        - Decrement the in-flight requests counter
        - If 'Retry-After' header is present: trigger global pause
        - If 'x-ratelimit-*' headers are present: Updates quota and optimizes pacing.
        """
        if headers is None:
            headers = {}

        with self._condition:

            # We have received a response, so decrement how many are in flight
            self.requests_in_flight -= 1

            if self.requests_in_flight < 0: self.requests_in_flight = 0 # Prevent going below zero

            retry_after = headers.get("Retry-After")

            if retry_after is not None:
                try:
                    retry_seconds = int(retry_after)
                    self.retry_after_timestamp = time.time() + retry_seconds
                    self.server_ratelimit_remaining = 0

                    print(f"Server responded with 'Retry-After' header. "
                          f"Informing all threads to pause for {retry_seconds}s.")
                    self._condition.notify_all()
                    return
                except ValueError:
                    pass # If header is malformed, proceed to standard logic

            remaining = headers.get("x-ratelimit-remaining")
            total = headers.get("x-ratelimit-total")
            prev_remaining = self.server_ratelimit_remaining

            if remaining is not None:
                self.server_ratelimit_remaining = int(remaining)
            if total is not None:
                self.server_ratelimit_total = int(total)

            # Calculate effective remaining after accounting for threads awaiting a response from the server
            effective_remaining = self.server_ratelimit_remaining - self.requests_in_flight

            if self.server_ratelimit_remaining > prev_remaining:
                print(f"Quota increased from ({prev_remaining} to {self.server_ratelimit_remaining}). "
                      f"Waking all threads to make their (paced) request.")
                self._condition.notify_all()

            elif effective_remaining > self.headroom:
                self._condition.notify_all()

            else:

                print(f"Only {effective_remaining} requests until we hit the rate limit."
                      f"Waking a single thread to make a (paced) request and check if the quota has been refreshed.")
                self._condition.notify()

