# Repository for "Rethinking API Rate Limiting: A Client-Side Approach"

This repository contains implementations used in *"Rethinking API Rate Limiting: A Client-Side Approach."*  
**Note:** The service worker implementation (in the `service_worker` folder) and the optimal MILP formulation (in `optimal.py`) were not used in the paper’s evaluation.

## Overview

This project includes multiple implementations of client-side rate limiting strategies, as well as auxiliary components like a backend compute service and a telemetry aggregator. The implementations cover different approaches:  
- **UB (Unlimited Backoff):** Pure exponential backoff.  
- **WB (Window-Based Backoff):** Exponential backoff combined with a sliding window limit.  
- **ATB (Adaptive Token Bucket):** Clients adapt their token generation rate based solely on their own success/failure.  
- **AATB (Assisted Adaptive Token Bucket):** Clients use an adaptive token bucket with external feedback from a controller.

## File Structure and Descriptions

- **service_worker**  
  Contains the Service Worker implementation of ATB along with necessary files as an example. The service worker intercepts fetch requests, queues them using an internal request queue mechanism, and communicates with the main page for status updates.  
  **Note:** Adjust your service worker and registration settings as needed.

- **optimal.py**  
  Contains a MILP formulation (using CPLEX) of the centralized rate-limiting problem. It assigns each request a processing time slot based on token bucket dynamics and minimizes the overall delay.  
  **Note:** This file was provided for further research and experimentation and was not used in the paper’s evaluation.

- **backend.py**  
  Implements a Quart-based web server that processes compute requests. The server logs incoming requests and returns the multiplication result of two numbers.  
  **Usage:** Run `backend.py` to start the server.

- **telemetry.py**  
  Implements a telemetry aggregator (using UDP) that collects and aggregates statistics from clients (e.g., total requests, congestion events, active clients) and sends aggregated feedback to the clients.

- **UB.py**  
  Implements the Unlimited Backoff (UB) strategy where clients use pure exponential backoff upon failure. This baseline approach sends requests immediately and retries with exponential delays.

- **WB.py**  
  Implements the Window-Based Backoff (WB) strategy that combines exponential backoff with a sliding window limit. This approach restricts the number of requests a client may send within a specified time window.

- **ATB.py**  
  Implements the Adaptive Token Bucket (ATB) algorithm, where clients adapt their token generation rate based on the success or failure of requests, without external feedback.

- **AATB.py**  
  Implements the AATB client that uses the Adaptive Token Bucket (from `AATB_TB.py`) to manage rate limiting with controller feedback. It updates its rate parameters accordingly.

- **AATB_TB.py**  
  Implements the Adaptive Token Bucket used by the Assisted Adaptive Token Bucket (AATB) algorithm. It handles token generation, adaptive rate increases/decreases, and communicates with a controller via UDP.

- **report_data.py**  
  Contains data classes (using Python's dataclasses) for reporting statistics and telemetry data exchanged between AATB clients and the controller.

## Important Configuration Considerations

- **IP Settings:**  
  Please verify all IP and algorithm configuration settings in your Envoy and other configuration files. Incorrect settings may cause unexpected behavior.

- **Algorithm Configurations:**  
  The adaptive algorithm parameters (e.g., bucket size, initial tokens, initial rate, rate increase steps, report windows) are defined in the respective configuration files. Adjust these values based on your deployment environment and experimental requirements.
  