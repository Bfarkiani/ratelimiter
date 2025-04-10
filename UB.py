"""
Client-side token bucket limiter with unlimited backoff (UB).
This client sends asynchronous requests to a remote server and uses an exponential backoff
strategy for failed attempts (using error codes like 429, 408, 425, 500, 502, 503, and 504).
Logs are maintained throughout the execution.
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

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, stream=sys.stdout)


@dataclass
class RequestData:
    req_id: int
    scheduled_time: float
    first_try: float
    success_time: float
    nr_of_attempts: int
    last_sleep: float


class Client:
    """
    Client performing asynchronous requests to a remote server.
    
    Attributes:
        id (int): Client identifier.
        requests (List[float]): List of scheduled local times to send each request.
        remote_addr (str): Remote server address.
        stamp (str): Timestamp string for logging and result storage.
        global_time (float): Global start time of the simulation.
    """
    def __init__(self, id: int, reqs: List[float], addr: str, stamp: str, global_time: float):
        self.requests = reqs
        # Initialize request status for each request using RequestData
        self.requests_status: Dict[int, RequestData] = {}
        logging.warning(f"client{id} requests: {self.requests}\n")
        for i, v in enumerate(reqs):
            self.requests_status[i] = RequestData(i, v, 0.0, 0.0, 0, 0.0)
        self.nr_of_reqs = len(reqs)
        self.id = id
        self.remote_addr = addr
        # Configure HTTP client with required headers and timeout settings.
        self.client = httpx.AsyncClient(http1=False, http2=True, timeout=10,
                                        headers={'content-type': 'application/json',
                                                 'X-Client-ID': str(self.id),
                                                 'x-api-key': 'xxx'})
        self.start_time = time.time()
        self.global_time = global_time
        self.stamp = stamp
        # Set a random maximum sleep time for backoff between 30 and 34 seconds.
        self.max_sleep = round(np.random.uniform(30, 34), 2)
        logging.warning(f"client{id} offset: {round(self.start_time - self.global_time, 2)}\n")

    async def send(self, req_id: int):
        """
        Sends a single request to the server and updates the request status.
        Returns True if the request was successful, otherwise False.
        """
        now = round(time.time() - self.start_time, 2)
        data = {'id': self.id, 'req_id': req_id, 'num1': 9, 'num2': 9}

        try:
            res = await self.client.post(self.remote_addr, json=data)
            now = round(time.time() - self.start_time, 2)
            glob = round(time.time() - self.global_time, 2)
            if res.status_code == 200:
                logging.warning(f"Client{self.id} successfully sent req {req_id} at L:{now} G:{glob}")
                req = self.requests_status[req_id]
                # Record first try time if this is the first attempt
                self.requests_status[req_id].first_try = now if req.first_try == 0 else req.first_try
                self.requests_status[req_id].nr_of_attempts += 1
                self.requests_status[req_id].success_time = now
                return True
            elif res.status_code in [429, 408, 425, 500, 502, 503, 504]:
                logging.warning(f"Client{self.id} failed req {req_id} with status {res.status_code} at L:{now} g:{glob}")
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
        Main asynchronous loop to send all requests. It waits until the scheduled time 
        and applies exponential backoff for failed attempts.
        """
        served_req = 0
        retry_error = 0
        while served_req < self.nr_of_reqs:
            now = round(time.time() - self.start_time, 2)
            # Wait until the scheduled time for the current request
            if now < self.requests_status[served_req].scheduled_time:
                await asyncio.sleep(self.requests_status[served_req].scheduled_time - now)
            try:
                res = await self.send(served_req)
                now = round(time.time() - self.start_time, 2)
                if res:
                    served_req += 1
                else:
                    # Exponential backoff calculation.
                    nrt = self.requests_status[served_req].nr_of_attempts
                    sleep_time = round(np.random.uniform(0.1, min((2 ** nrt) - 1, self.max_sleep)), 2)
                    self.requests_status[served_req].last_sleep = sleep_time
                    logging.warning(f"Client{self.id} sleeps {sleep_time} seconds at {now}")
                    await asyncio.sleep(sleep_time)

            except Exception as e:
                logging.error(f"Client{self.id} encountered connection error at request {served_req}: {str(e)} at {now}")
                logging.exception("Full traceback:")
                retry_error += 1
                if retry_error > 10:
                    logging.error(f"Client{self.id} failed to connect to server. Aborting...")
                    break
                await asyncio.sleep(1)

        logging.warning(f"Finished client{self.id}")
        await self.client.aclose()
        self.log_results()

    def log_results(self):
        """
        Logs results by writing successful request statistics to a client-specific file.
        """
        dir_name = f"results_UB_{self.stamp}"
        client_log = f"client_{self.id}_.txt"
        offset = self.start_time - self.global_time
        with open(os.path.join(dir_name, client_log), "w") as f:
            for k, v in self.requests_status.items():
                if v.success_time > 0:  # Only log successful requests
                    f.write(f"{v.req_id},{round(v.scheduled_time + offset, 2)},{round(v.first_try + offset, 2)},"
                            f"{round(v.success_time + offset, 2)},{v.nr_of_attempts},{self.id}\n")


def client_wrapper(id: int, reqs: List[float], addr: str, stamp: str, global_time: float):
    """
    Wrapper to create and run a client in a dedicated event loop.
    Allows starting clients in parallel with a deterministic delay.
    """
    time.sleep((id * 0.5) % 10)  # Start clients with a delay based on their id.
    client = Client(id, reqs, addr, stamp, global_time)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(client.run())

    try:
        loop.run_until_complete(task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logging.warning(f"Client{id} cancelled")
        client.log_results()
    finally:
        if not task.done():
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
        loop.close()


async def main(all_reqs, addr, stamp):
    """
    Main asynchronous function to start all client tasks using a process pool.
    It tracks a global start time and waits for all clients to finish.
    """
    loop = asyncio.get_running_loop()
    all_tasks = []
    global_time = time.time()
    # Determine number of clients from the request dictionary.
    clients = len(all_reqs)
    with ProcessPoolExecutor(max_workers=clients) as pool:
        for c in range(clients):
            all_tasks.append(loop.run_in_executor(pool, client_wrapper, c, all_reqs[c], addr, stamp, global_time))
        try:
            await asyncio.gather(*all_tasks)
        except KeyboardInterrupt:
            logging.warning("Terminating tasks...")
            for t in all_tasks:
                if not t.done():
                    t.cancel()
                try:
                    loop.run_until_complete(t)
                except asyncio.CancelledError:
                    pass


def process_result(stamp: str, total_reqs: int, clients: int):
    """
    Aggregates individual client logs into a summary and writes detailed statistics
    including response times and attempt counts.
    """
    import pandas as pd
    aggregated_df = []
    dir_name = f"results_UB_{stamp}"

    for file in os.listdir(dir_name):
        try:
            if file.startswith("client_"):
                with open(os.path.join(dir_name, file), "r") as f:
                    df = pd.read_csv(f, index_col=False, header=None)
                    df.columns = ['id', 'original_scheduled_time', 'first_try', 'success_time', 'nr_of_attempts', 'client_id']
                    aggregated_df.append(df)
        except Exception as e:
            logging.error(f"error opening {os.path.join(dir_name, file)}: \n{e}")

    aggregated_df = pd.concat(aggregated_df, ignore_index=True)
    aggregated_df.to_csv(os.path.join(dir_name, "Summary_clients.txt"), index=False)

    with open(os.path.join(dir_name, "Summary_stats.txt"), "w") as f:
        logging.warning("\nSummary statistics written to Summary_stats.txt...\n")
        f.write(f"Number_of_clients:\t{clients}\n")
        logging.warning(f"Number_of_clients:\t{clients}")
        f.write(f"Total_Requests:\t{len(aggregated_df)}\n")
        logging.warning(f"Total_Requests:\t{len(aggregated_df)}")
        f.write(f"Simulation_Duration:\t{max(aggregated_df['success_time']) - min(aggregated_df['first_try'])}\n")
        logging.warning(f"Simulation_Duration:\t{max(aggregated_df['success_time']) - min(aggregated_df['first_try'])}")
        f.write(f"Max_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).max()}\n")
        logging.warning(f"Max_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).max()}")
        f.write(f"Min_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).min()}\n")
        logging.warning(f"Min_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).min()}")
        f.write(f"AVG_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).mean()}\n")
        logging.warning(f"AVG_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).mean()}")
        f.write(f"STD_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).std()}\n")
        logging.warning(f"STD_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).std()}")

        f.write(f"Max_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).max()}\n")
        logging.warning(f"Max_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).max()}")
        f.write(f"Min_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).min()}\n")
        logging.warning(f"Min_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).min()}")
        f.write(f"AVG_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).mean()}\n")
        logging.warning(f"AVG_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).mean()}")
        f.write(f"STD_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).std()}\n")
        logging.warning(f"STD_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).std()}")
        f.write(f"Max_Attempts:\t{aggregated_df['nr_of_attempts'].max()}\n")
        logging.warning(f"Max_Attempts:\t{aggregated_df['nr_of_attempts'].max()}")
        f.write(f"Min_Attempts:\t{aggregated_df['nr_of_attempts'].min()}\n")
        logging.warning(f"Min_Attempts:\t{aggregated_df['nr_of_attempts'].min()}")
        f.write(f"Avg_Attempts:\t{aggregated_df['nr_of_attempts'].mean()}\n")
        logging.warning(f"Avg_Attempts:\t{aggregated_df['nr_of_attempts'].mean()}")
        f.write(f"STD_Attempts:\t{aggregated_df['nr_of_attempts'].std()}\n")
        logging.warning(f"STD_Attempts:\t{aggregated_df['nr_of_attempts'].std()}")
        f.write(f"Total_Attempts:\t{aggregated_df['nr_of_attempts'].sum()}\n")
        logging.warning(f"Total_Attempts:\t{aggregated_df['nr_of_attempts'].sum()}")
        f.write(f"Total_Attempts_per_req:\t{aggregated_df['nr_of_attempts'].sum() / total_reqs}\n")
        logging.warning(f"Total_Attempts_per_req:\t{aggregated_df['nr_of_attempts'].sum() / total_reqs}")
        f.write(f"Total_updates:\t0\n")
        logging.warning(f"Total_updates:\t0")


if __name__ == '__main__':
    import argparse
    import datetime
    import shutil

    now = datetime.datetime.now()
    stamp = now.strftime("%d_%H_%M")

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default="config_UB.json", help='Path to config_UB.json')
    parser.add_argument('--sim-dir', type=str, default="sims/sim1", help='Path to simulation directory containing requests.txt')
    parser.add_argument('--stamp', type=str, default=str(stamp), help='Timestamp for output directory name')
    args = parser.parse_args()
    stamp = args.stamp if args.stamp else stamp

    logging.warning(f"Reading config from {args}")
    # Read configuration file for remote server details.
    try:
        with open(args.config) as f:
            data = json.load(f)
            remote_server = data.get('remote_server', '127.0.0.1')
            remote_port = data.get('remote_port', 5000)
            logging.warning("Unlimited client with no config!")
    except Exception as e:
        logging.error(f"Error opening {args.config}: \n{e}")
        raise

    addr = f"http://{remote_server}:{str(remote_port)}/compute"
    logging.warning(f"addr: {addr}")

    # Read client requests from the simulation directory.
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

    clients = len(all_reqs)  # Number of clients inferred from requests file
    logging.warning(f"total requests: {total_req}")
    logging.warning(f"max request time: {max_tim}")
    logging.warning(f"number of clients: {clients}")

    # Create the results directory.
    dir_name = f"results_UB_{stamp}"
    os.makedirs(dir_name, exist_ok=True)

    try:
        asyncio.run(main(all_reqs, addr, stamp))
    except KeyboardInterrupt as e:
        logging.error(f"simulation interrupted...: \n{e}")
    except Exception as e:
        logging.error(f"error during simulation...: \n{e}")
    finally:
        logging.warning("finished simulation, wait for processing results...")
        process_result(stamp, total_req, clients)
        shutil.copyfile(args.config, os.path.join(dir_name, os.path.basename(args.config)))
