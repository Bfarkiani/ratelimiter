#ATB implementation. Please check the paper. 
import asyncio
import json
import random
import time
from typing import List, Dict
import logging
from concurrent.futures import ProcessPoolExecutor

import httpx
import numpy as np
import os
from dataclasses import dataclass
import sys

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, stream=sys.stdout)

@dataclass
class RequestData:
    req_id: int
    scheduled_time: float
    first_try: float
    success_time: float
    nr_of_attempts: int
    last_sleep: float

class TokenBucket:
    """A token bucket for adaptive rate control."""
    def __init__(self, size: float, tokens: float, rate: float, start_time: float, initial_congestion: float):
        self.tokens = tokens
        self.bucket_size = size
        self.rate = convert_rate(rate)  # convert to tokens per second
        self.start_time = start_time
        self.last_used = start_time
        self.last_congestion_rate = initial_congestion
        logging.info(f"TokenBucket initialized at {start_time} with params: size:{size}, rate:{rate}, tokens:{tokens}")

    async def acquire(self):
        """Acquire a token from the bucket; wait if necessary."""
        generated_tokens = (time.time() - self.last_used) * self.rate
        self.tokens = min(self.bucket_size, self.tokens + generated_tokens)
        if self.tokens >= 1.0:
            self.tokens -= 1
            self.last_used = round(time.time(), 2)
            return True
        remaining = (1 - self.tokens) / self.rate
        await asyncio.sleep(remaining)
        self.tokens = max(self.tokens - 1, 0)
        self.last_used = round(time.time(), 2)
        return True

    def increase(self, first_step, second_step):
        """
        Increase the token generation rate based on success.
        Returns the new rate and the step used.
        """
        min_increase = 0.01
        if self.rate < self.last_congestion_rate:
            new_rate = round(max(self.rate + min_increase, self.rate * first_step), 2)
            step = 1
        else:
            new_rate = round(max(self.rate + min_increase, self.rate * second_step), 2)
            step = 2
        self.rate = new_rate
        return new_rate, step

    def decrease(self, rate):
        """
        Decrease the token generation rate upon failure.
        Returns the new rate.
        """
        self.last_congestion_rate = self.rate
        new_rate = max(0.01, round(self.rate / 2.0, 2))
        self.tokens = 0
        self.rate = new_rate
        return new_rate

class Client:
    """Client implementing Adaptive Token Bucket mechanism for sending requests."""
    def __init__(self, id: int, reqs: List[int], addr: str, stamp: str, window: int, bucket_size: float,
                 initial_tokens: float, initial_rate: float, initial_congestion: float,
                 first_step: float, second_step: float, third_step: float, global_time: float):
        self.requests = reqs
        self.requests_status: Dict[int, RequestData] = {}
        logging.info(f"client{id} requests: {self.requests}\n")
        for i, v in enumerate(reqs):
            self.requests_status[i] = RequestData(i, v, 0.0, 0.0, 0, 0.0)
        self.nr_of_reqs = len(reqs)
        self.id = id
        self.remote_addr = addr
        self.window = window
        self.client = httpx.AsyncClient(http1=False, http2=True, timeout=10,
                                        headers={'content-type': 'application/json',
                                                 'X-Client-ID': str(self.id),
                                                 'x-api-key': 'xxx'})
        self.start_time = time.time()
        self.global_time = global_time
        self.sent_during_window = 0
        self.stamp = stamp
        self.max_sleep = round(np.random.uniform(30, 34), 2)
        self.initial_rate = initial_rate
        self.token_bucket = TokenBucket(bucket_size, initial_tokens, self.initial_rate, self.start_time, initial_congestion)
        self.first_step = first_step
        self.second_step = second_step
        self.third_step = third_step
        self.last_rate_adjust = 0
        logging.info(f"client{id} offset: {round(self.start_time - self.global_time, 2)}\n")

    async def send(self, req_id: int):
        """Acquire a token and send a request to the server."""
        now = round(time.time() - self.start_time, 2)
        data = {'id': self.id, 'req_id': req_id, 'num1': 9, 'num2': 9}
        await self.token_bucket.acquire()
        try:
            res = await self.client.post(self.remote_addr, json=data)
            now = round(time.time() - self.start_time, 2)
            glob = round(time.time() - self.global_time, 2)
            if res.status_code == 200:
                logging.info(f"Client{self.id} successfully sent req {req_id} at L:{now} G:{glob}")
                req = self.requests_status[req_id]
                self.requests_status[req_id].first_try = now if req.first_try == 0 else req.first_try
                self.requests_status[req_id].nr_of_attempts += 1
                self.requests_status[req_id].success_time = now
                return True
            elif res.status_code in [429, 408, 425, 500, 502, 503, 504]:
                logging.info(f"Client{self.id} failed req {req_id} with status {res.status_code} at L:{now} G:{glob}")
                req = self.requests_status[req_id]
                self.requests_status[req_id].first_try = now if req.first_try == 0 else req.first_try
                self.requests_status[req_id].nr_of_attempts += 1
                return False

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.WriteError) as e:
            logging.error(str(e))
            raise e

    async def run(self):
        """Run the client's request loop until all scheduled requests are served."""
        served_req = 0
        retry_error = 0
        sleep_time = 0
        while served_req < self.nr_of_reqs:
            now = round(time.time() - self.start_time, 2)
            if now < self.requests_status[served_req].scheduled_time:
                await asyncio.sleep(self.requests_status[served_req].scheduled_time - now)
            try:
                res = await self.send(served_req)
                now = round(time.time() - self.start_time, 2)
                if res:
                    served_req += 1
                    old_rate = self.token_bucket.rate
                    new_rate, step = self.token_bucket.increase(self.first_step, self.second_step)
                    logging.info(f"At {now} rate of client{self.id} increased from {old_rate} to {new_rate} - step {step}")
                    self.last_rate_adjust = now
                else:
                    old_rate = self.token_bucket.rate
                    new_rate = self.token_bucket.decrease(self.initial_rate)
                    logging.info(f"At {now} rate of client{self.id} decreased from {old_rate} to {new_rate} and will wait for {sleep_time}s")
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
        """Log the results of the client's requests to a file."""
        dir_name = f"results_ATB_{self.stamp}"
        client_log = f"client_{self.id}_.txt"
        offset = self.start_time - self.global_time
        with open(os.path.join(dir_name, client_log), "w") as f:
            for k, v in self.requests_status.items():
                if v.success_time > 0:
                    f.write(f"{v.req_id},{round(v.scheduled_time+offset,2)},{round(v.first_try+offset,2)},{round(v.success_time+offset,2)},{v.nr_of_attempts},{self.id}\n")

def convert_rate(rate: float):
    """Convert a rate from per minute to per second, with a minimum rate."""
    return max(round(rate / 60, 2), 0.01)

def client_wrapper(id: int, reqs: List[int], addr: str, stamp: str, window, bucket_size: float, initial_tokens: float,
                   initial_rate: float, initial_congestion: float, first_step: float, second_step: float,
                   third_step: float, global_time):
    """Wrapper to run a client in its own event loop with a deterministic start delay."""
    time.sleep((id * 0.5) % 10)
    client = Client(id, reqs, addr, stamp, window, bucket_size, initial_tokens, initial_rate,
                    initial_congestion, first_step, second_step, third_step, global_time)
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

async def main(all_reqs, addr, stamp, window, bucket_size, initial_tokens, initial_rate, initial_congestion,
               first_step, second_step, third_step):
    """Launch all clients using a process pool."""
    loop = asyncio.get_running_loop()
    all_tasks = []
    global_time = time.time()
    with ProcessPoolExecutor(max_workers=clients) as pool:
        for c in range(clients):
            all_tasks.append(loop.run_in_executor(pool, client_wrapper, c, all_reqs[c], addr, stamp, window,
                                                   bucket_size, initial_tokens, initial_rate, initial_congestion,
                                                   first_step, second_step, third_step, global_time))
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
    """Aggregate client logs and write summary statistics to files."""
    import pandas as pd
    aggregated_df = []
    dir_name = f"results_ATB_{stamp}"
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
        logging.info("\nSummary statistics written to Summary_stats.txt...\n")
        f.write(f"Number_of_clients:\t{clients}\n")
        logging.info(f"Number_of_clients:\t{clients}")
        f.write(f"Total_Requests:\t{len(aggregated_df)}\n")
        logging.info(f"Total_Requests:\t{len(aggregated_df)}")
        f.write(f"Simulation_Duration:\t{max(aggregated_df['success_time']) - min(aggregated_df['first_try'])}\n")
        logging.info(f"Simulation_Duration:\t{max(aggregated_df['success_time']) - min(aggregated_df['first_try'])}")
        f.write(f"Max_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).max()}\n")
        logging.info(f"Max_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).max()}")
        f.write(f"Min_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).min()}\n")
        logging.info(f"Min_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).min()}")
        f.write(f"AVG_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).mean()}\n")
        logging.info(f"AVG_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).mean()}")
        f.write(f"STD_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).std()}\n")
        logging.info(f"STD_Response_Time:\t{(aggregated_df['success_time'] - aggregated_df['original_scheduled_time']).std()}")
        f.write(f"Max_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).max()}\n")
        logging.info(f"Max_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).max()}")
        f.write(f"Min_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).min()}\n")
        logging.info(f"Min_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).min()}")
        f.write(f"AVG_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).mean()}\n")
        logging.info(f"AVG_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).mean()}")
        f.write(f"STD_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).std()}\n")
        logging.info(f"STD_Service_Time:\t{(aggregated_df['success_time'] - aggregated_df['first_try']).std()}")
        f.write(f"Max_Attempts:\t{aggregated_df['nr_of_attempts'].max()}\n")
        logging.info(f"Max_Attempts:\t{aggregated_df['nr_of_attempts'].max()}")
        f.write(f"Min_Attempts:\t{aggregated_df['nr_of_attempts'].min()}\n")
        logging.info(f"Min_Attempts:\t{aggregated_df['nr_of_attempts'].min()}")
        f.write(f"Avg_Attempts:\t{aggregated_df['nr_of_attempts'].mean()}\n")
        logging.info(f"Avg_Attempts:\t{aggregated_df['nr_of_attempts'].mean()}")
        f.write(f"STD_Attempts:\t{aggregated_df['nr_of_attempts'].std()}\n")
        logging.info(f"STD_Attempts:\t{aggregated_df['nr_of_attempts'].std()}")
        f.write(f"Total_Attempts:\t{aggregated_df['nr_of_attempts'].sum()}\n")
        logging.info(f"Total_Attempts:\t{aggregated_df['nr_of_attempts'].sum()}")
        f.write(f"Total_Attempts_per_req:\t{aggregated_df['nr_of_attempts'].sum() / total_reqs}\n")
        logging.info(f"Total_Attempts_per_req:\t{aggregated_df['nr_of_attempts'].sum() / total_reqs}")
        f.write(f"Total_updates:\t0\n")
        logging.info(f"Total_updates:\t0")

if __name__ == '__main__':
    import argparse
    import datetime
    import shutil
    parser = argparse.ArgumentParser()
    now = datetime.datetime.now()
    stamp = now.strftime("%d_%H_%M")
    parser.add_argument('--config', type=str, default="config_ATB.json", help='Path to config_ATB.json')
    parser.add_argument('--sim-dir', type=str, default="simulations/sim_20250109_200211/simulation_0", help='Path to simulation directory containing requests.txt')
    parser.add_argument('--stamp', type=str, default=str(stamp), help='Timestamp for output directory name')
    args = parser.parse_args()
    stamp = args.stamp if args.stamp else stamp
    logging.info(f"Reading config from {args}")
    try:
        with open(args.config) as f:
            data = json.load(f)
            window = int(data.get('window', 60))
            bucket_size = float(data.get('bucket_size', 4))
            initial_tokens = float(data.get('initial_tokens', 1))
            initial_rate = float(data.get('initial_rate', 1))
            initial_congestion = float(data.get('initial_congestion', 60))
            first_step = float(data.get('first_step', 1.5))
            second_step = float(data.get('second_step', 1.2))
            third_step = float(data.get('third_step', 1.1))
            remote_server = data.get('remote_server', '127.0.0.1')
            remote_port = int(data.get('remote_port', 5000))
            logging.info(f"Config parameters: window={window}, bucket_size={bucket_size}, initial_tokens={initial_tokens}, "
                         f"initial_rate={initial_rate}, initial_congestion={initial_congestion}, first_step={first_step}, "
                         f"second_step={second_step}, third_step={third_step}, remote_server={remote_server}, remote_port={remote_port}")
    except Exception as e:
        logging.error(f"Error opening {args.config}: \n{e}")
        raise
    addr = f"http://{remote_server}:{str(remote_port)}/compute"
    logging.info(f"addr: {addr}")
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
    dir_name = f"results_ATB_{stamp}"
    os.makedirs(dir_name, exist_ok=True)
    try:
        asyncio.run(main(all_reqs, addr, stamp, window, bucket_size, initial_tokens, initial_rate,
                           initial_congestion, first_step, second_step, third_step))
    except KeyboardInterrupt as e:
        logging.error(f"simulation interrupted...: \n{e}")
    except Exception as e:
        logging.error(f"error during simulation...: \n{e}")
    finally:
        logging.info("finished simulation, wait for processing results...")
        process_result(stamp, total_req, clients)
        shutil.copyfile(args.config, os.path.join(dir_name, os.path.basename(args.config)))
