#Telemetry server that controls client rates in AATB.
import asyncio
import socket
import time
import threading
from collections import deque
from dataclasses import asdict
import logging
import report_data
import json
import sys
import traceback
import argparse

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, stream=sys.stdout)


class RateController:
    """UDP-based telemetry aggregator that collects and sends aggregated stats."""
    def __init__(self, size_b_clients, size_b_total, token_generation_rate, addr=('0.0.0.0', 8080)):
        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.setblocking(False)

        self.generation_rate = token_generation_rate

        # Buffers for storing stats over a window
        self.buffer_total_requests = deque([0] * size_b_total, maxlen=size_b_total)
        self.current_requests = 0
        self.running_sum_requests = 0

        self.buffer_total_429 = deque([0] * size_b_total, maxlen=size_b_total)
        self.current_429 = 0
        self.running_sum_429 = 0

        self.buffer_active_clients = deque([set() for _ in range(size_b_clients)], maxlen=size_b_clients)
        self.current_clients = set()
        self.running_sum_clients = 0

        self.stat_time = time.time()

    async def _total_clients(self, cleanup_period):
        """Periodically update the running total of active clients."""
        while True:
            logging.info(f"summing clients at {round(time.time() - self.stat_time, 2)}")
            self.buffer_active_clients.append(self.current_clients)
            if self.buffer_active_clients:
                self.running_sum_clients = len(set().union(*self.buffer_active_clients))
            else:
                self.running_sum_clients = 0
            self.current_clients = set()
            await asyncio.sleep(cleanup_period)

    async def _total_reqs(self, cleanup_period):
        """Periodically update total requests statistics."""
        while True:
            logging.info(f"summing requests at {round(time.time() - self.stat_time, 2)}")
            self.running_sum_requests -= self.buffer_total_requests[0]
            self.buffer_total_requests.append(self.current_requests)
            self.running_sum_requests += self.current_requests
            self.current_requests = 0
            await asyncio.sleep(cleanup_period)

    async def _congestion_add(self, cleanup_period):
        """Periodically update the total number of congestion (429) occurrences."""
        while True:
            logging.info(f"summing 429 at {round(time.time() - self.stat_time, 2)}")
            self.running_sum_429 -= self.buffer_total_429[0]
            self.buffer_total_429.append(self.current_429)
            self.running_sum_429 += self.current_429
            self.current_429 = 0
            await asyncio.sleep(cleanup_period)

    async def run(self):
        """Main loop: receive incoming UDP messages and process them."""
        try:
            while True:
                loop = asyncio.get_running_loop()
                data, addr = await loop.sock_recvfrom(self.sock, 1024)
                if data:
                    data = json.loads(data.decode())
                    logging.info(f'received data {data}')
                    await self.send_data(data, addr, loop)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Run error: {e}")

    def stop(self):
        """Stop the server by closing the socket."""
        self.sock.close()

    async def send_data(self, data, addr, loop):
        """Aggregate the received data, compute totals, and send a response."""
        logging.info(f'send data {data} {addr}')
        try:
            id = data.get('id')
            if id is None:
                raise Exception('id is None')
            id = int(id)
            sent = data.get('sent')
            if sent is None:
                raise Exception('sent is None')
            sent = int(sent)
            if data.get('type') == 'cong':
                self.current_429 += 1

            self.current_clients.add(id)
            self.current_requests += sent

            c_total_active_clients = self.running_sum_clients if self.running_sum_clients > 0 else 1
            logging.info(f'buffer total requests {self.buffer_total_requests}')
            c_total_request = self.running_sum_requests
            c_total_429 = self.running_sum_429

        except Exception as e:
            logging.error(f"error parsing data {data}")
            return

        send_data = report_data.ReceivedData(
            total_requests=c_total_request,
            active_clients=c_total_active_clients,
            reported_429=c_total_429,
            token_rate=self.generation_rate
        )

        try:
            payload = json.dumps(asdict(send_data)).encode()
            logging.info(f"Sending data {payload} to client{id}")
            await loop.sock_sendto(self.sock, payload, addr)
        except (KeyError, json.JSONDecodeError) as e:
            logging.error(f"Data processing error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in send_data: {e}")


async def run_controller(period_active, period_total, size_b_total, size_b_clients, token_generation_rate):
    """Initialize and run the server along with its statistics update tasks."""
    controller = RateController(
        size_b_clients=size_b_clients,
        size_b_total=size_b_total,
        token_generation_rate=token_generation_rate
    )

    tasks = [
        asyncio.create_task(controller.run(), name='main'),
        asyncio.create_task(controller._total_clients(period_active), name='total_clients'),
        asyncio.create_task(controller._total_reqs(period_total), name='total_reqs'),
        asyncio.create_task(controller._congestion_add(period_total), name='total_429')
    ]

    try:
        logging.info("Telemetry server is running...")
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt. Canceling tasks...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        print("Caught cancellation.")
    except Exception as e:
        logging.error(f"Unexpected error in main: {e} \n {traceback.format_exc()}")
    finally:
        controller.stop()

def parse_args():
    """Parse command line arguments for the server."""
    parser = argparse.ArgumentParser(description='Telemetry server with configurable parameters')
    parser.add_argument('--period-active', type=int, default=10,
                        help='Period for active client updates (default: 10)')
    parser.add_argument('--period-total', type=int, default=2,
                        help='Period for total stats updates (default: 2)')
    parser.add_argument('--token-rate', type=int, default=80,
                        help='Token generation rate (default: 80)')
    parser.add_argument('--window', type=int, default=30,
                        help='Window size in seconds (default: 30)')
    return parser.parse_args()

def main():
    """Main entry point for the telemetry server."""
    args = parse_args()

    # Calculate buffer sizes based on the provided window and periods.
    size_b_total = round(args.window / args.period_total)
    size_b_clients = round(args.window * 2 / args.period_active)

    try:
        asyncio.run(run_controller(
            period_active=args.period_active,
            period_total=args.period_total,
            size_b_total=size_b_total,
            size_b_clients=size_b_clients,
            token_generation_rate=args.token_rate
        ))
    except KeyboardInterrupt:
        logging.info("Terminating...")
        time.sleep(1)
        sys.exit(0)

if __name__ == '__main__':
    main()
