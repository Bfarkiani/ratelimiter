#AATB client. check the paper
import asyncio
import json
import random
import time
from typing import List, Dict, Tuple
import logging
from concurrent.futures import ProcessPoolExecutor
import httpx
import numpy as np
import os
from dataclasses import dataclass
import AATB_TB
import sys

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.WARNING, stream=sys.stdout)

@dataclass
class RequestData:
    req_id: int
    scheduled_time: float
    first_try: float
    success_time: float
    nr_of_attempts: int
    last_sleep: float

class Client:
    """AATB client that communicates with a controller for congestion feedback."""
    def __init__(self, id: int, reqs: List[float], addr: str, stamp: str,
                 bucket_size: float, initial_tokens: float, initial_rate: float,
                 global_time: float, first_step: float, second_step: float,
                 report_window: int = 10, controller_addr: Tuple[str, int] = ('192.168.170.129', 8080)):
        self.requests = reqs
        self.requests_status: Dict[int, RequestData] = {}
        logging.warning(f"client{id} requests: {self.requests}\n")
        for i, v in enumerate(reqs):
            self.requests_status[i] = RequestData(i, v, 0.0, 0.0, 0, 0.0)
        self.nr_of_reqs = len(reqs)
        self.id = id
        self.remote_addr = addr
        self.client = httpx.AsyncClient(http1=False, http2=True, timeout=10,
                                        headers={'content-type': 'application/json',
                                                 'X-Client-ID': str(self.id),
                                                 'x-api-key': 'xxx'})
        self.start_time = time.time()
        self.global_time = global_time
        self.sent_during_window = 0
        self.stamp = stamp
        self.max_sleep = round(np.random.uniform(30, 34), 2)
        self.token_bucket = AATB_TB.AdaptiveTokenBucket(
            size=bucket_size,
            tokens=initial_tokens,
            start_time=self.start_time,
            controller_addr=controller_addr,
            initial_rate=initial_rate,
            id=self.id,
            report_window=report_window,
            step_one=first_step,
            step_two=second_step
        )
        logging.info(f"client{id} offset: {round(self.start_time - self.global_time, 2)}\n")

    async def send(self, req_id: int):
        """Acquire a token and send a request; adjust rate based on response."""
        now = round(time.time() - self.start_time, 2)
        data = {'id': self.id, 'req_id': req_id, 'num1': 9, 'num2': 9}
        await self.token_bucket.acquire()
        try:
            res = await self.client.post(self.remote_addr, json=data)
            self.token_bucket._increase_sent()
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
                await self.token_bucket.congestion_notification(asyncio.get_event_loop())
                return False
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.WriteError) as e:
            logging.error(str(e))
            raise e

    async def run(self):
        """Run the request loop until all scheduled requests are processed."""
        served_req = 0
        retry_error = 0
        logging.warning(f"client{self.id} started at {round(time.time() - self.start_time,2)}")
        while served_req < self.nr_of_reqs:
            now = round(time.time() - self.start_time, 2)
            if now < self.requests_status[served_req].scheduled_time:
                await asyncio.sleep(self.requests_status[served_req].scheduled_time - now)
            try:
                res = await self.send(served_req)
                now = round(time.time() - self.start_time, 2)
                if res:
                    served_req += 1
            except Exception as e:
                logging.error(f"Client{self.id} encountered connection error at request {served_req}: {str(e)} at {now}")
                logging.exception("Full traceback:")
                retry_error += 1
                if retry_error > 10:
                    logging.error(f"Client{self.id} failed to connect to server. Aborting...")
                    break
                await asyncio.sleep(1)
        logging.warning(f"Finished client{self.id}")
        self.token_bucket.stop()
        await self.client.aclose()
        await asyncio.sleep(1)
        self.log_results()

    def log_results(self):
        """Log the client's successful requests."""
        dir_name = f"results_AATB_{self.stamp}"
        client_log = f"client_{self.id}_.txt"
        offset = self.start_time - self.global_time
        updates_log = f"updates_client_{self.id}"
        with open(os.path.join(dir_name, updates_log), "w") as f:
            f.write(f"{self.token_bucket.update_counter}\n")
        with open(os.path.join(dir_name, client_log), "w") as f:
            for k, v in self.requests_status.items():
                if v.success_time > 0:
                    f.write(f"{v.req_id},{round(v.scheduled_time + offset,2)},{round(v.first_try + offset,2)},"
                            f"{round(v.success_time + offset,2)},{v.nr_of_attempts},{self.id}\n")

def convert_rate(rate: float):
    """Convert a rate from per minute to per second."""
    return max(round(rate / 60, 2), 0.01)

def client_wrapper(id: int, reqs: List[float], addr: str, stamp: str, bucket_size: float,
                   initial_tokens: float, initial_rate: float, first_step: float, second_step: float,
                   report_window, controller_addr, global_time: float):
    """Wrapper to run a client in its own event loop with a deterministic start delay."""
    time.sleep((id * 0.5) % 10)
    client = Client(id=id, reqs=reqs, addr=addr, stamp=stamp, bucket_size=bucket_size,
                    initial_tokens=initial_tokens, initial_rate=initial_rate, first_step=first_step,
                    second_step=second_step, report_window=report_window, controller_addr=controller_addr,
                    global_time=global_time)
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

async def main(all_reqs: Dict[int, List[float]], addr: str, stamp: str, bucket_size: float,
               initial_tokens: float, initial_rate: float, first_step: float, second_step: float,
               report_window: int, controller_addr: Tuple[str, int]):
    """Launch all AATB clients concurrently using a process pool."""
    loop = asyncio.get_running_loop()
    all_tasks = []
    global_time = time.time()
    with ProcessPoolExecutor(max_workers=clients) as pool:
        for c in range(clients):
            all_tasks.append(loop.run_in_executor(pool, client_wrapper, c, all_reqs[c], addr, stamp,
                                                   bucket_size, initial_tokens, initial_rate, first_step,
                                                   second_step, report_window, controller_addr, global_time))
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
    """Aggregate client logs and write summary statistics."""
    import pandas as pd
    aggregated_df = []
    dir_name = f"results_AATB_{stamp}"
    total_updates = 0
    for file in os.listdir(dir_name):
        try:
            if file.startswith("client_"):
                with open(os.path.join(dir_name, file), "r") as f:
                    df = pd.read_csv(f, index_col=False, header=None)
                    df.columns = ['id', 'original_scheduled_time', 'first_try', 'success_time',
                                  'nr_of_attempts', 'client_id']
                    aggregated_df.append(df)
            if file.startswith("updates_client_"):
                with open(os.path.join(dir_name, file), "r") as f:
                    total_updates += int(f.readline())
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
        f.write(f"Total_updates:\t{total_updates}\n")
        logging.info(f"Total_updates:\t{total_updates}")

if __name__ == '__main__':
    import argparse
    import datetime
    import shutil
    parser = argparse.ArgumentParser()
    now = datetime.datetime.now()
    stamp = now.strftime("%d_%H_%M")
    parser.add_argument('--config', type=str, default="config_AATB.json", help='Path to config_AATB.json')
    parser.add_argument('--sim-dir', type=str, default="simulations/sim_20250109_200211/simulation_0", help='Path to simulation directory containing requests.txt')
    parser.add_argument('--stamp', type=str, default=str(stamp), help='Timestamp for output directory name')
    args = parser.parse_args()
    stamp = args.stamp if args.stamp else stamp
    logging.warning(f"Reading config from {args}")
    try:
        with open(args.config) as f:
            data = json.load(f)
            bucket_size = float(data.get('bucket_size', 4))
            initial_tokens = float(data.get('initial_tokens', 1))
            initial_rate = float(data.get('initial_rate', 1))
            first_step = float(data.get('first_step', 1.5))
            second_step = float(data.get('second_step', 1.2))
            remote_server = data.get('remote_server', '127.0.0.1')
            remote_port = int(data.get('remote_port', 5000))
            controller_ip = data.get('controller_ip', '127.0.0.1')
            controller_port = int(data.get('controller_port', 8080))
            report_window = int(data.get('report_window', 10))
            logging.warning(
                f"Config parameters: bucket_size={bucket_size}, initial_tokens={initial_tokens}, "
                f"initial_rate={initial_rate}, first_step={first_step}, second_step={second_step}, "
                f"remote_server={remote_server}, remote_port={remote_port}, "
                f"controller_addr={(controller_ip,controller_port)}, report window={report_window}")
    except Exception as e:
        logging.error(f"Error opening {args.config}: \n{e}")
        raise
    addr = f"http://{remote_server}:{str(remote_port)}/compute"
    logging.warning(f"addr: {addr}")
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
    logging.warning(f"total requests: {total_req}")
    logging.warning(f"max request time: {max_tim}")
    logging.warning(f"number of clients: {clients}")
    dir_name = f"results_AATB_{stamp}"
    os.makedirs(dir_name, exist_ok=True)
    try:
        asyncio.run(main(all_reqs=all_reqs, addr=addr, stamp=stamp, bucket_size=bucket_size,
                         initial_tokens=initial_tokens, initial_rate=initial_rate,
                         first_step=first_step, second_step=second_step, report_window=report_window,
                         controller_addr=(controller_ip, controller_port)))
    except KeyboardInterrupt as e:
        logging.error(f"simulation interrupted...: \n{e}")
    except Exception as e:
        logging.error(f"error during simulation...: \n{e}")
    finally:
        logging.warning(f"finished simulation, wait for processing results...")
        process_result(stamp, total_req, clients)
        shutil.copyfile(args.config, os.path.join(dir_name, os.path.basename(args.config)))
