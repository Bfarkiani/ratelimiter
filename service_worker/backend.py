#backend service that does a simple computation
import os
import signal
import asyncio
from quart import Quart, render_template, request
from pathlib import Path
import logging
import json
import time

# Setup Quart app and logging configuration.
app = Quart(__name__, template_folder=os.path.join(os.curdir, 'index'))
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# Dictionary to store the number of messages received per client.
clients = {}

@app.route("/")
async def index():
    """Render and return the index page."""
    return await render_template("index.html")

@app.route("/status", methods=['GET'])
async def status():
    """Return simple status OK."""
    return "OK", 200

@app.route("/compute", methods=['POST'])
async def compute():
    """
    Receive a computation request.
    Expect JSON data with 'id', 'num1', 'num2', and 'req_id'. 
    Updates client request count and returns multiplication result.
    """
    data = await request.get_json()
    if data:
        try:
            client = data.get('id')
            num1 = int(data.get('num1', 3))
            num2 = int(data.get('num2', 3))
            req_id = data.get('req_id')
            if client is None or req_id is None:
                return "Data is not complete", 400
            clients[client] = clients.get(client, 0) + 1
            logging.info(f"request {clients[client]-1} of client {client} has received at {round(time.time()-start,2)}s")
            return {"result": num1 * num2}
        except Exception as e:
            logging.info(f"error parsing data: {e}")
            return "Data is bad", 400
    return "No data", 400

if __name__ == '__main__':
    start = time.time()
    from hypercorn import Config
    from hypercorn.asyncio import serve

    config = Config()
    config.h2_enabled = True
    config.bind = ['0.0.0.0:5000']
    config.workers = os.cpu_count()
    config.backlog = 2048
    config.keep_alive_timeout = 350

    logging.info("starting server on http://0.0.0.0:5000")

    try:
        asyncio.run(serve(app, config))
    except KeyboardInterrupt:
        logging.info("exiting....")
    finally:
        import datetime
        now = datetime.datetime.now()
        formatted_time = now.strftime("%d_%H_%M")
        with open('backend_log_' + formatted_time + '.txt', mode='w+') as f:
            for k, v in clients.items():
                f.write(f"{k}\t{v}\n")
