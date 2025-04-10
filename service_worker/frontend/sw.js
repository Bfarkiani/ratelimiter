const TOKEN_RATE = 4.0; // Starting rate per minute (matches initial_rate in Python)
const BUCKET_SIZE = 4.0; // Match bucket_size from Python
const INITIAL_TOKENS = 1.0; // Match initial_tokens from Python
const INCREASE_STEP_ONE = 1.2; // Match first_step from Python
const INCREASE_STEP_TWO = 1.2; // Match second_step from Python
																
const INITIAL_CONGESTION_RATE = 20.0; // Match initial_congestion from Python

let globalRequestCounter = 0;
let keepAliveInterval;

function swLog(message, type = 'info') {
    console.log(`[ServiceWorker] ${message}`);
    clients.matchAll().then(clients => {
        clients.forEach(client => {
            client.postMessage({
                type: 'log',
                message: `[SW] ${message}`,
                logType: type
            });
        });
    });
}

class TokenBucket {
    constructor() {
        this.tokens = INITIAL_TOKENS;
        this.rate = TOKEN_RATE / 60; // Convert to per-second
        this.bucketSize = BUCKET_SIZE;
        this.lastUpdate = Date.now();
        this.lastCongestionRate = INITIAL_CONGESTION_RATE / 60; // Initial congestion rate in per-second
        swLog(`TokenBucket initialized: rate=${(this.rate * 60).toFixed(2)}/min, bucketSize=${this.bucketSize}, tokens=${this.tokens}`);
    }

	acquire() {
		const now = Date.now();
		const elapsedSeconds = (now - this.lastUpdate) / 1000;
		
		const generatedTokens = elapsedSeconds * this.rate;
		this.tokens = Math.min(this.tokens + generatedTokens, this.bucketSize);
		
		swLog(`Token check: ${this.tokens.toFixed(3)} tokens available (rate: ${(this.rate * 60).toFixed(2)}/min)`);
		
		if (this.tokens >= 1.0) {
			this.tokens -= 1.0;
			this.lastUpdate = now;
			swLog(`Token acquired, ${this.tokens.toFixed(3)} tokens remaining`);
			return true;
		}
		
		swLog(`Not enough tokens (${this.tokens.toFixed(3)}), waiting for more`, 'info');
		return false;
	}

    adjustRateUp() {
        const minIncrease = 0.01; 
        let newRate, stepUsed;

        if (this.rate < this.lastCongestionRate) {
            newRate = Math.max(this.rate + minIncrease, this.rate * INCREASE_STEP_ONE);
            stepUsed = 'stepOne';
        } else {
            newRate = Math.max(this.rate + minIncrease, this.rate * INCREASE_STEP_TWO);
            stepUsed = 'stepTwo';

        }
        
        const oldRate = this.rate * 60;
        this.rate = newRate;
        swLog(`Rate increased from ${oldRate.toFixed(2)}/min to ${(this.rate * 60).toFixed(2)}/min (${stepUsed} step used)`);
        return newRate;
    }

    adjustRateDown() {
        const oldRate = this.rate * 60;
        this.lastCongestionRate = this.rate;
        const newRate = Math.max(0.01, this.rate / 2.0);
        this.rate = newRate;
        this.tokens = 0;
        
        swLog(`Rate decreased from ${oldRate.toFixed(2)}/min to ${(this.rate * 60).toFixed(2)}/min (bucket emptied)`, 'error');
        return newRate;
    }
	
}

class QueuedRequest {
    constructor(request, resolver) {
        this.requestId = ++globalRequestCounter;
        this.request = request.clone();
        this.originalScheduledTime = Date.now();
        this.firstTryTime = 0;
        this.scheduledTime = Date.now();
        this.retries = 0;
        this.backoffTime = 1000; // 1 second default backoff
        this.resolver = resolver;
        this.isProcessing = false;
        this.hasNotifiedFailure = false;
        this.extractRequestData();
    }

    async extractRequestData() {
        try {
            const clone = this.request.clone();
            const body = await clone.json();
            const relativeTime = (this.scheduledTime - this.originalScheduledTime) / 1000;
            swLog(`Request #${this.requestId} queued for T+${relativeTime.toFixed(1)}s`);
        } catch (e) {
            swLog(`Error extracting request data for request #${this.requestId}: ${e}`, 'error');
        }
    }

    getMetadata() {
        const now = Date.now();
        return {
            requestId: this.requestId,
            retries: this.retries,
            totalTimeInQueue: now - this.originalScheduledTime,
            firstTryTime: this.firstTryTime > 0 ? this.firstTryTime - this.originalScheduledTime : 0
        };
    }
}

class RequestQueue {
    constructor() {
        this.queue = new Map();
        this.processing = false;
        swLog('RequestQueue initialized');
        this.startProcessing();
    }

    addRequest(request, resolver) {
        const queuedRequest = new QueuedRequest(request, resolver);
        const id = `req-${queuedRequest.requestId}`;
        this.queue.set(id, queuedRequest);
        return id;
    }

    async startProcessing() {
        while (true) {
            if (this.queue.size > 0 && !this.processing) {
                await this.processQueue();
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    async processQueue() {
        if (this.processing) return;
        this.processing = true;

        while (this.queue.size > 0) {
            const [id, queuedRequest] = this.queue.entries().next().value;
            const now = Date.now();
            
            if (now < queuedRequest.scheduledTime) {
                await new Promise(resolve => setTimeout(resolve, 100));
                continue;
            }

            let hasToken = tokenBucket.acquire();
            while (!hasToken) {
                await new Promise(resolve => setTimeout(resolve, 100));
                hasToken = tokenBucket.acquire();
            }

            try {
                queuedRequest.isProcessing = true;
                
                // Track first try time if this is the first attempt
                if (queuedRequest.retries === 0) {
                    queuedRequest.firstTryTime = now;
                }
                
                const relativeTime = (now - queuedRequest.originalScheduledTime) / 1000;
                
                if (queuedRequest.retries === 0) {
                    swLog(`Sending request #${queuedRequest.requestId} (T+${relativeTime.toFixed(1)}s)`);
                } else {
                    swLog(`Retrying request #${queuedRequest.requestId} (attempt ${queuedRequest.retries + 1}, T+${relativeTime.toFixed(1)}s)`);
                }

                const response = await fetch(queuedRequest.request.clone());

                if (response.ok) {
                    // Success - increase rate
                    tokenBucket.adjustRateUp();

                    const responseData = await response.clone().json();
                    const metadata = queuedRequest.getMetadata();
                    swLog(`Request #${queuedRequest.requestId} completed successfully after ${(metadata.totalTimeInQueue/1000).toFixed(2)}s (retries: ${metadata.retries})`, 'success');
                    
                    queuedRequest.resolver(new Response(JSON.stringify({
                        ...responseData,
                        _metadata: metadata
                    }), {
                        status: 200,
                        headers: { 'Content-Type': 'application/json' }
                    }));
                    
                    this.queue.delete(id);
                } else {
                    // Failure - decrease rate
                    tokenBucket.adjustRateDown();

                    queuedRequest.retries++;
                    queuedRequest.scheduledTime = now + queuedRequest.backoffTime;
                    
                    if (!queuedRequest.hasNotifiedFailure) {
                        const errorResponse = await response.clone().text();
                        const metadata = queuedRequest.getMetadata();
                        queuedRequest.resolver(new Response(JSON.stringify({
                            error: errorResponse,
                            status: 'retrying',
                            _metadata: metadata
                        }), { 
                            status: response.status,
                            headers: { 'Content-Type': 'application/json' }
                        }));
                        queuedRequest.hasNotifiedFailure = true;
                    }

                    const relativeRetryTime = (queuedRequest.scheduledTime - queuedRequest.originalScheduledTime) / 1000;
                    swLog(`Request #${queuedRequest.requestId} failed, rescheduling for T+${relativeRetryTime.toFixed(1)}s with backoff 1s`);
                }
            } catch (error) {
                swLog(`Error processing request #${queuedRequest.requestId}: ${error}`, 'error');
                const metadata = queuedRequest.getMetadata();
                queuedRequest.resolver(new Response(JSON.stringify({
                    error: error.message,
                    _metadata: metadata
                }), { 
                    status: 500,
                    headers: { 'Content-Type': 'application/json' }
                }));
                this.queue.delete(id);
            } finally {
                queuedRequest.isProcessing = false;
            }

            await new Promise(resolve => setTimeout(resolve, 100));
        }

        this.processing = false;
    }
}

const tokenBucket = new TokenBucket();
const requestQueue = new RequestQueue();

self.addEventListener('install', (event) => {
    event.waitUntil(
        Promise.resolve()
            .then(() => {
                swLog('Service Worker installing...', 'info');
                return self.skipWaiting();
            })
            .then(() => {
                swLog('Service Worker install complete', 'success');
                startKeepAlive();
            })
    );
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        Promise.resolve()
            .then(() => {
                swLog('Service Worker activating...', 'info');
                return clients.claim();
            })
            .then(() => {
                swLog('Service Worker activation complete', 'success');
                startKeepAlive();
            })
    );
});

function startKeepAlive() {
    if (keepAliveInterval) {
        clearInterval(keepAliveInterval);
    }
    keepAliveInterval = setInterval(() => {
        self.registration.update();
        clients.matchAll().then(clients => {
            clients.forEach(client => {
                client.postMessage({ type: 'keepAlive', timestamp: Date.now() });
            });
        });
    }, 20000);
}

self.addEventListener('message', (event) => {
    if (event.data && event.data.type === 'keepAliveResponse') {
        swLog('Received keepalive response from client');
    }
});

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);
    
    if (url.pathname === '/compute' && event.request.method === 'POST') {
        event.respondWith(
            new Promise(resolve => {
                requestQueue.addRequest(event.request, resolve);
            })
        );
    }
});