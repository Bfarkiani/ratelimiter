"""
Client-side windows-based exponential backoff (WB).
This client cannot send more requests than specified (limit) within a given window duration.
If the rate limit is reached or an error occurs, it applies an exponential backoff delay.
Logs are written for every action.
"""

import asyncio
import json
import time
from typing import List, Dict
import logging
from concurrent.futures import ProcessPoolExecutor

import httpx
import numpy as np
import os
from dataclasses import dataclass
import sys

# Configure logging output to STDOUT
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, stream=sys.stdout)


@dataclass
class RequestData:
    req_id: int
    scheduled_time: float   # Scheduled local time for the request
    first_try: float        # Time of first attempt
    success_time: float     # Time when the request succeeded
    nr_of_attempts: int     # Total attempts made
    last_sleep: float       # Last backoff sleep time used


class Client:
    """
    Client using a sliding window limiter with exponential backoff.

    Args:
        id (int): The client identifier.
        reqs (List[float]): A list of scheduled local times at which this client sends a request.
        addr (str): The remote server address.
        stamp (str): Timestamp for output file names.
        window (int): Duration of the sliding window in seconds.
        limit (int): Maximum number of allowed successful requests within the window.
        global_time (float): Global start time of the simulation.
    """
    def __init__(self, id: int, reqs: List[float], addr: str, stamp: str, window: int, limit: int, global_time: float):
        self.requests = reqs
        # Initialize request status for each scheduled request
        self.requests_status: Dict[int, RequestData] = {}
        logging.info(f"client{id} requests: {self.requests}\n")
        for i, v in enumerate(reqs):
            self.requests_status[i] = RequestData(i, v, 0.0, 0.0, 0, 0.0)
        self.nr_of_reqs = len(reqs)
        self.id = id
        self.remote_addr = addr
        # Create an asynchronous HTTP client with appropriate headers and timeouts
        self.client = httpx.AsyncClient(http1=False, http2=True, timeout=10,
                                        headers={'content-type': 'application/json',
                                                 'X-Client-ID': str(self.id),
                                                 'x-api-key': 'xxx'})
        self.start_time = time.time()
        self.global_time = global_time
        self.window = window  # Window length in seconds
        self.limit = limit    # Maximum allowed requests per window
        self.stamp = stamp
        # Random maximum sleep (backoff) time between 30 and 34 seconds
        self.max_sleep = round(np.random.uniform(30, 34), 2)
        logging.info(f"client{id} offset: {round(self.start_time - self.global_time, 2)}\n")

    async def send(self, req_id: int) -> bool:
        """
        Sends a single request to the server and applies sliding window limitations.
        Returns True if the request is successful, otherwise False.
        """
        now = round(time.time() - self.start_time, 2)
        data = {'id': self.id, 'req_id': req_id, 'num1': 9, 'num2': 9}

        # Determine the start of the sliding window interval
        window_start = max(round(now - self.window, 2), 0)
        # Collect the timestamps of successful requests that fall within the current window
        success_times = [
            req.success_time
            for req in self.requests_status.values()
            if req.success_time > 0 and req.success_time > window_start
        ]

        # If the number of successes has reached the limit, wait until the oldest one expires
        if len(success_times) >= self.limit:
            oldest_success = min(success_times)
            sleep_time = round(oldest_success + self.window - now, 2)
            logging.info(f"At {now} client{self.id} sleeps for {sleep_time} seconds – waiting for request that succeeded at {oldest_success} to expire")
            await asyncio.sleep(sleep_time)

        # Try sending the request
        try:
            res = await self.client.post(self.remote_addr, json=data)
            now = round(time.time() - self.start_time, 2)
            glob = round(time.time() - self.global_time, 2)
            if res.status_code == 200:
                logging.info(f"Client{self.id} successfully sent req {req_id} at L:{now} G:{glob}")
                req = self.requests_status[req_id]
                # Record the time of the first attempt if not already recorded
                self.requests_status[req_id].first_try = now if req.first_try == 0 else req.first_try
                self.requests_status[req_id].nr_of_attempts += 1
                self.requests_status[req_id].success_time = now
                return True
            elif res.status_code in [429, 408, 425, 500, 502, 503, 504]:
                logging.error(f"Client{self.id} failed req {req_id} with status {res.status_code} at L:{now} G:{glob}")
                req = self.requests_status[req_id]
                self.requests_status[req_id].first_try = now if req.first_try == 0 else req.first_try
                self.requests_status[req_id].nr_of_attempts += 1
                return False

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.WriteError) as e:
            logging.error(str(e))
            raise e

    async def run(self):
        """
        Main asynchronous loop to process all scheduled requests.
        It waits for each request's scheduled time, applies backoff delays on failure,
        and sends the request.
        """
        served_req = 0
        retry_error = 0
        while served_req < self.nr_of_reqs:
            now = round(time.time() - self.start_time, 2)
            # Wait until the scheduled time of the current request
            if now < self.requests_status[served_req].scheduled_time:
                await asyncio.sleep(self.requests_status[served_req].scheduled_time - now)
            try:
                res = await self.send(served_req)
                now = round(time.time() - self.start_time, 2)
                if res:
                    served_req += 1
                else:
                    # Exponential backoff calculation using a randomized delay
                    nrt = self.requests_status[served_req].nr_of_attempts
                    sleep_time = round(np.random.uniform(0.1, min((2 ** nrt) - 1, self.max_sleep)), 2)
                    self.requests_status[served_req].last_sleep = sleep_time
                    logging.info(f"Client{self.id} sleeps {sleep_time} seconds at {now}")
                    await asyncio.sleep(sleep_time)

            except Exception as e:
                logging.error(f"Client{self.id} encountered connection error at request {served_req}: {str(e)} at {now}")
                logging.exception("Full traceback:")
                retry_error += 1
                if retry_error > 10:
                    logging.error(f"Client{self.id} failed to connect to server. Aborting...")
                    break
                await asyncio.sleep(1)

        logging.info(f"Finished client{self.id}")
        await self.client.aclose()
        self.log_results()

    def log_results(self):
        """
        Writes successful request statistics to a client-specific log file.
        """
        dir_name = f"results_WB_{self.stamp}"
        client_log = f"client_{self.id}_.txt"
        offset = self.start_time - self.global_time
        with open(os.path.join(dir_name, client_log), "w") as f:
            for k, v in self.requests_status.items():
                if v.success_time > 0:  # Only log successful requests
                    f.write(f"{v.req_id},{round(v.scheduled_time + offset, 2)},{round(v.first_try + offset, 2)},"
                            f"{round(v.success_time + offset, 2)},{v.nr_of_attempts},{self.id}\n")


def client_wrapper(id: int, reqs: List[float], addr: str, window: int, limit: int, stamp: str, global_time: float):
    """
    Wrapper to run a client on its own event loop.
    Introduces a deterministic delay based on the client ID.
    """
    time.sleep((id * 0.5) % 10)
    client = Client(id, reqs, addr, stamp, window, limit, global_time)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(client.run())

    try:
        loop.run_until_complete(task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logging.info(f"Client{id} cancelled")
        client.log_results()
    finally:
        if not task.done():
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
        loop.close()

async def main(all_reqs, addr, window, limit, stamp):
    """
    Main asynchronous function to launch all client processes.
    Uses a process pool to run clients concurrently.
    """
    loop = asyncio.get_running_loop()
    all_tasks = []
    global_time = time.time()
    num_clients = len(all_reqs)
    with ProcessPoolExecutor(max_workers=num_clients) as pool:
        for c in range(num_clients):
            all_tasks.append(loop.run_in_executor(
                pool, client_wrapper, c, all_reqs[c], addr, window, limit, stamp, global_time))
        try:
            await asyncio.gather(*all_tasks)
        except KeyboardInterrupt:
            logging.info("Terminating tasks...")
            for t in all_tasks:
                if not t.done():
                    t.cancel()
                try:
                    loop.run_until_complete(t)
                except asyncio.CancelledError:
                    pass


def process_result(stamp: str, total_reqs: int, clients: int):
    """
    Aggregates individual client logs to generate summary statistics.
    The summarized data is written to CSV and text files.
    """
    import pandas as pd
    aggregated_df = []
    dir_name = f"results_WB_{stamp}"

    for file in os.listdir(dir_name):
        try:
            if file.startswith("client_"):
                with open(os.path.join(dir_name, file), "r") as f:
                    df = pd.read_csv(f, index_col=False, header=None)
                    df.columns = ['id', 'original_scheduled_time', 'first_try', 'success_time', 'nr_of_attempts', 'client_id']
                    aggregated_df.append(df)
        except Exception as e:
            logging.error(f"Error opening {os.path.join(dir_name, file)}: \n{e}")

    aggregated_df = pd.concat(aggregated_df, ignore_index=True)
    aggregated_df.to_csv(os.path.join(dir_name, "Summary_clients.txt"), index=False)

    with open(os.path.join(dir_name, "Summary_stats.txt"), "w") as f:
        logging.info("\nSummary statistics written to Summary_stats.txt...\n")
        f.write(f"Number_of_clients:\t{clients}\n")
        logging.info(f"Number_of_clients:\t{clients}")
        f.write(f"Total_Requests:\t{len(aggregated_df)}\n")
        logging.info(f"Total_Requests:\t{len(aggregated_df)}")
        duration = max(aggregated_df['success_time']) - min(aggregated_df['first_try'])
        f.write(f"Simulation_Duration:\t{duration}\n")
        logging.info(f"Simulation_Duration:\t{duration}")
        max_resp = (aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).max()
        f.write(f"Max_Response_Time:\t{max_resp}\n")
        logging.info(f"Max_Response_Time:\t{max_resp}")
        min_resp = (aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).min()
        f.write(f"Min_Response_Time:\t{min_resp}\n")
        logging.info(f"Min_Response_Time:\t{min_resp}")
        avg_resp = (aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).mean()
        f.write(f"AVG_Response_Time:\t{avg_resp}\n")
        logging.info(f"AVG_Response_Time:\t{avg_resp}")
        std_resp = (aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).std()
        f.write(f"STD_Response_Time:\t{std_resp}\n")
        logging.info(f"STD_Response_Time:\t{std_resp}")

        max_service = (aggregated_df['success_time'] - aggregated_df['first_try']).max()
        f.write(f"Max_Service_Time:\t{max_service}\n")
        logging.info(f"Max_Service_Time:\t{max_service}")
        min_service = (aggregated_df['success_time'] - aggregated_df['first_try']).min()
        f.write(f"Min_Service_Time:\t{min_service}\n")
        logging.info(f"Min_Service_Time:\t{min_service}")
        avg_service = (aggregated_df['success_time'] - aggregated_df['first_try']).mean()
        f.write(f"AVG_Service_Time:\t{avg_service}\n")
        logging.info(f"AVG_Service_Time:\t{avg_service}")
        std_service = (aggregated_df['success_time'] - aggregated_df['first_try']).std()
        f.write(f"STD_Service_Time:\t{std_service}\n")
        logging.info(f"STD_Service_Time:\t{std_service}")

        max_attempts = aggregated_df['nr_of_attempts'].max()
        f.write(f"Max_Attempts:\t{max_attempts}\n")
        logging.info(f"Max_Attempts:\t{max_attempts}")
        min_attempts = aggregated_df['nr_of_attempts'].min()
        f.write(f"Min_Attempts:\t{min_attempts}\n")
        logging.info(f"Min_Attempts:\t{min_attempts}")
        avg_attempts = aggregated_df['nr_of_attempts'].mean()
        f.write(f"Avg_Attempts:\t{avg_attempts}\n")
        logging.info(f"Avg_Attempts:\t{avg_attempts}")
        std_attempts = aggregated_df['nr_of_attempts'].std()
        f.write(f"STD_Attempts:\t{std_attempts}\n")
        logging.info(f"STD_Attempts:\t{std_attempts}")
        total_attempts = aggregated_df['nr_of_attempts'].sum()
        f.write(f"Total_Attempts:\t{total_attempts}\n")
        logging.info(f"Total_Attempts:\t{total_attempts}")
        f.write(f"Total_Attempts_per_req:\t{total_attempts / total_reqs}\n")
        logging.info(f"Total_Attempts_per_req:\t{total_attempts / total_reqs}")
        f.write(f"Total_updates:\t0\n")
        logging.info(f"Total_updates:\t0")


if __name__ == '__main__':
    import argparse
    import datetime
    import shutil

    now = datetime.datetime.now()
    stamp = now.strftime("%d_%H_%M")

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default="config_WB.json", help='Path to config_WB.json')
    parser.add_argument('--sim-dir', type=str, default="sims/sim1", help='Path to simulation directory containing requests.txt')
    parser.add_argument('--stamp', type=str, default=str(stamp), help='Timestamp for output directory name')
    args = parser.parse_args()
    stamp = args.stamp if args.stamp else stamp

    logging.info(f"Reading config from {args}")
    try:
        with open(args.config) as f:
            data = json.load(f)
            window = int(data.get('window', 60))
            limit = int(data.get('limit', 4))
            remote_server = data.get('remote_server', '127.0.0.1')
            remote_port = data.get('remote_port', 5000)
            logging.info(f"Config parameters: window={window}, limit={limit}")
    except Exception as e:
        logging.error(f"Error opening {args.config}: \n{e}")
        raise

    addr = f"http://{remote_server}:{str(remote_port)}/compute"
    logging.info(f"addr: {addr}")

    # Read requests from the simulation directory
    requests_file = os.path.join(args.sim_dir, "requests.txt")
    all_reqs = {}
    total_req = 0
    max_tim = 0

    try:
        with open(requests_file, 'r') as f:
            for line in f:
                client_id, num_reqs, timestamps = line.strip().split('\t')
                client_id = int(client_id)
                timestamps = [float(t) for t in timestamps.split(',')]
                all_reqs[client_id] = timestamps
                total_req += len(timestamps)
                max_tim = max(max_tim, max(timestamps))
    except Exception as e:
        logging.error(f"Error reading requests from {requests_file}: \n{e}")
        raise

    clients = len(all_reqs)
    logging.info(f"total requests: {total_req}")
    logging.info(f"max request time: {max_tim}")
    logging.info(f"number of clients: {clients}")

    # Create the results directory
    dir_name = f"results_WB_{stamp}"
    os.makedirs(dir_name, exist_ok=True)

    try:
        asyncio.run(main(all_reqs, addr, window, limit, stamp))
    except KeyboardInterrupt as e:
        logging.error(f"simulation interrupted...: \n{e}")
    except Exception as e:
        logging.error(f"error during simulation...: \n{e}")
    finally:
        logging.info("finished simulation, wait for processing results...")
        process_result(stamp, total_req, clients)
        # Use os.path.basename for cross-platform compatibility
        shutil.copyfile(args.config, os.path.join(dir_name, os.path.basename(args.config)))
