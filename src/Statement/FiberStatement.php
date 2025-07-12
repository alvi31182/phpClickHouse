<?php

namespace ClickHouseDB\Statement;

use ClickHouseDB\Statement;
use ClickHouseDB\Transport\CurlerRequest;
use ClickHouseDB\Transport\FiberHandler;
use Fiber;

/**
 * Async Statement implementation using PHP Fibers
 * Provides true async/await pattern for ClickHouse queries
 */
class FiberStatement extends Statement
{
    /**
     * @var FiberHandler
     */
    private $fiberHandler;

    /**
     * @var string
     */
    private $requestId;

    /**
     * @var bool
     */
    private $isExecuted = false;

    /**
     * @var Fiber|null
     */
    private $resultFiber = null;

    public function __construct(CurlerRequest $request, FiberHandler $fiberHandler, string $requestId)
    {
        parent::__construct($request);
        $this->fiberHandler = $fiberHandler;
        $this->requestId = $requestId;
    }

    /**
     * Wait for the async result to be available
     */
    public function await(): self
    {
        if ($this->isExecuted) {
            return $this;
        }

        // Create a fiber to wait for the result
        $this->resultFiber = new Fiber(function () {
            while (!$this->fiberHandler->isRequestCompleted($this->requestId)) {
                Fiber::suspend();
            }
            
            // Get the completed request
            $completedRequest = $this->fiberHandler->getCompletedRequest($this->requestId);
            if ($completedRequest) {
                $this->_request = $completedRequest;
            }
        });

        $this->resultFiber->start();
        
        // Process the fiber until completion
        while ($this->resultFiber && !$this->resultFiber->isTerminated()) {
            if ($this->resultFiber->isSuspended()) {
                $this->resultFiber->resume();
            }
        }

        $this->isExecuted = true;
        return $this;
    }

    /**
     * Check if the async result is ready
     */
    public function isReady(): bool
    {
        return $this->fiberHandler->isRequestCompleted($this->requestId);
    }

    /**
     * Get the request ID
     */
    public function getRequestId(): string
    {
        return $this->requestId;
    }

    /**
     * Override parent methods to ensure await is called
     */
    public function rows(): array
    {
        $this->await();
        return parent::rows();
    }

    public function fetchOne($key = null)
    {
        $this->await();
        return parent::fetchOne($key);
    }

    public function fetchRow($key = null)
    {
        $this->await();
        return parent::fetchRow($key);
    }

    public function count(): int
    {
        $this->await();
        return parent::count();
    }

    public function countAll()
    {
        $this->await();
        return parent::countAll();
    }

    public function totals(): array
    {
        $this->await();
        return parent::totals();
    }

    public function extremes(): array
    {
        $this->await();
        return parent::extremes();
    }

    public function extremesMin(): array
    {
        $this->await();
        return parent::extremesMin();
    }

    public function extremesMax(): array
    {
        $this->await();
        return parent::extremesMax();
    }

    public function statistics($key = false)
    {
        $this->await();
        return parent::statistics($key);
    }

    public function info(): array
    {
        $this->await();
        return parent::info();
    }

    public function info_upload(): array
    {
        $this->await();
        return parent::info_upload();
    }

    public function totalTimeRequest(): float
    {
        $this->await();
        return parent::totalTimeRequest();
    }

    public function rawData()
    {
        $this->await();
        return parent::rawData();
    }

    public function jsonRows(): array
    {
        $this->await();
        return parent::jsonRows();
    }

    public function rowsAsTree($path): array
    {
        $this->await();
        return parent::rowsAsTree($path);
    }

    public function isError(): bool
    {
        $this->await();
        return parent::isError();
    }

    public function error()
    {
        $this->await();
        return parent::error();
    }

    /**
     * Get the underlying fiber handler
     */
    public function getFiberHandler(): FiberHandler
    {
        return $this->fiberHandler;
    }
} 