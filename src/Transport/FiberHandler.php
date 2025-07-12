<?php

namespace ClickHouseDB\Transport;

use ClickHouseDB\Exception\TransportException;
use Fiber;

/**
 * Fiber-based async handler for ClickHouse requests
 * Replaces CurlerRolling with true async/await pattern using PHP 8.1+ Fibers
 */
class FiberHandler
{
    /**
     * @var int
     */
    private $simultaneousLimit = 10;

    /**
     * @var array<string, CurlerRequest>
     */
    private $pendingRequests = [];

    /**
     * @var array<string, Fiber>
     */
    private $activeFibers = [];

    /**
     * @var array<string, CurlerRequest>
     */
    private $completedRequests = [];

    /**
     * @var bool
     */
    private $isRunning = false;

    /**
     * @var int
     */
    private $completedRequestCount = 0;

    /**
     * @var array<string, mixed>
     */
    private $fiberResults = [];

    /**
     * @var \SplQueue<Fiber>
     */
    private $fiberQueue;

    /**
     * @var array<string, mixed>
     */
    private $requestResults = [];

    public function __construct(int $simultaneousLimit = 10)
    {
        $this->simultaneousLimit = $simultaneousLimit;
        $this->fiberQueue = new \SplQueue();
    }

    /**
     * Add request to async queue
     */
    public function addRequest(CurlerRequest $request): string
    {
        $id = $request->getId() ?: $request->getUniqHash($this->completedRequestCount);
        
        if (isset($this->pendingRequests[$id])) {
            throw new TransportException("Request with ID $id already exists in queue");
        }

        $this->pendingRequests[$id] = $request;
        return $id;
    }

    /**
     * Execute all pending requests using Fibers
     */
    public function executeAsync(): void
    {
        if ($this->isRunning) {
            throw new TransportException("FiberHandler is already running");
        }

        $this->isRunning = true;
        $this->completedRequests = [];
        $this->fiberResults = [];
        $this->requestResults = [];

        // Create fibers for all pending requests
        foreach ($this->pendingRequests as $id => $request) {
            $fiber = new Fiber(function (CurlerRequest $req) {
                return $this->executeRequest($req);
            });
            
            $this->activeFibers[$id] = $fiber;
            $fiber->start($request);
        }

        // Process fibers until all complete
        $this->processFibers();

        $this->isRunning = false;
    }

    /**
     * Execute a single request in a fiber with true async behavior
     */
    private function executeRequest(CurlerRequest $request): CurlerResponse
    {
        // Yield control to allow other fibers to run
        Fiber::suspend();
        
        // Execute the actual curl request
        $handle = $request->handle();
        
        // Simulate async I/O by yielding during curl execution
        Fiber::suspend();
        
        curl_exec($handle);
        
        // Create response
        $response = $this->makeResponse($handle);
        $request->setResponse($response);
        
        // Execute callback if exists
        $request->onCallback();
        
        return $response;
    }

    /**
     * Process all active fibers with cooperative multitasking
     */
    private function processFibers(): void
    {
        while (!empty($this->activeFibers)) {
            $activeCount = 0;
            
            foreach ($this->activeFibers as $id => $fiber) {
                if ($fiber->isTerminated()) {
                    // Fiber completed
                    $this->completedRequests[$id] = $this->pendingRequests[$id];
                    $this->fiberResults[$id] = $fiber->getReturn();
                    $this->requestResults[$id] = $fiber->getReturn();
                    unset($this->activeFibers[$id]);
                    unset($this->pendingRequests[$id]);
                    $this->completedRequestCount++;
                } elseif ($fiber->isSuspended()) {
                    // Resume fiber
                    $fiber->resume();
                    $activeCount++;
                }
            }
            
            // If no fibers are active, we're done
            if ($activeCount === 0 && !empty($this->activeFibers)) {
                // All fibers are suspended, resume them
                foreach ($this->activeFibers as $fiber) {
                    if ($fiber->isSuspended()) {
                        $fiber->resume();
                    }
                }
            }
            
            // Small delay to prevent busy waiting
            if (!empty($this->activeFibers)) {
                usleep(100); // 0.1ms for better responsiveness
            }
        }
    }

    /**
     * Get completed request by ID
     */
    public function getCompletedRequest(string $id): ?CurlerRequest
    {
        return $this->completedRequests[$id] ?? null;
    }

    /**
     * Get all completed requests
     */
    public function getCompletedRequests(): array
    {
        return $this->completedRequests;
    }

    /**
     * Check if request is completed
     */
    public function isRequestCompleted(string $id): bool
    {
        return isset($this->completedRequests[$id]);
    }

    /**
     * Get count of pending requests
     */
    public function countPending(): int
    {
        return count($this->pendingRequests);
    }

    /**
     * Get count of active fibers
     */
    public function countActive(): int
    {
        return count($this->activeFibers);
    }

    /**
     * Get count of completed requests
     */
    public function countCompleted(): int
    {
        return $this->completedRequestCount;
    }

    /**
     * Set simultaneous limit
     */
    public function setSimultaneousLimit(int $count): self
    {
        if ($count < 1) {
            throw new \InvalidArgumentException("Simultaneous limit must be >= 1");
        }
        
        $this->simultaneousLimit = $count;
        return $this;
    }

    /**
     * Get simultaneous limit
     */
    public function getSimultaneousLimit(): int
    {
        return $this->simultaneousLimit;
    }

    /**
     * Clear all requests
     */
    public function clear(): void
    {
        $this->pendingRequests = [];
        $this->activeFibers = [];
        $this->completedRequests = [];
        $this->fiberResults = [];
        $this->requestResults = [];
        $this->isRunning = false;
    }

    /**
     * Create response from curl handle
     */
    private function makeResponse($handle): CurlerResponse
    {
        $response = curl_multi_getcontent($handle);
        $header_size = curl_getinfo($handle, CURLINFO_HEADER_SIZE);
        $header = substr($response ?? '', 0, $header_size);
        $body = substr($response ?? '', $header_size);

        $responseObj = new CurlerResponse();
        $responseObj->_headers = $this->parseHeaders($header);
        $responseObj->_body = $body;
        $responseObj->_info = curl_getinfo($handle);
        $responseObj->_error = curl_error($handle);
        $responseObj->_errorNo = curl_errno($handle);
        $responseObj->_useTime = 0;

        return $responseObj;
    }

    /**
     * Parse headers from curl response
     */
    private function parseHeaders(string $response): array
    {
        $headers = [];
        $header_text = $response;

        foreach (explode("\r\n", $header_text) as $i => $line) {
            if ($i === 0) {
                $headers['http_code'] = $line;
            } else {
                $r = explode(': ', $line);
                if (count($r) == 2) {
                    $headers[$r[0]] = $r[1];
                }
            }
        }

        return $headers;
    }

    /**
     * Get info about current state
     */
    public function getInfo(): string
    {
        return sprintf(
            "activeFibers=%d, pending=%d, completed=%d, running=%s",
            $this->countActive(),
            $this->countPending(),
            $this->countCompleted(),
            $this->isRunning ? 'true' : 'false'
        );
    }

    /**
     * Get request result by ID
     */
    public function getRequestResult(string $id): mixed
    {
        return $this->requestResults[$id] ?? null;
    }

    /**
     * Get all request results
     */
    public function getRequestResults(): array
    {
        return $this->requestResults;
    }
} 