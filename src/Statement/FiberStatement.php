<?php

namespace ClickHouseDB\Statement;

use ClickHouseDB\Exception\TransportException;
use ClickHouseDB\Exception\DatabaseException;
use ClickHouseDB\Exception\QueryException;
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

        // Wait for the request to complete
        while (!$this->fiberHandler->isRequestCompleted($this->requestId)) {
            usleep(1000); // 1ms delay
        }
        
        // Get the completed request
        $completedRequest = $this->fiberHandler->getCompletedRequest($this->requestId);
        if (!$completedRequest) {
            throw new TransportException("Request not found after completion");
        }

        // Create a new Statement with the completed request
        $completedStatement = new Statement($completedRequest);
        
        // Copy the completed statement's data to this instance
        $this->copyStatementData($completedStatement);

        $this->isExecuted = true;
        return $this;
    }

    /**
     * Copy data from another statement instance
     */
    private function copyStatementData(\ClickHouseDB\Statement $source): void
    {
        // Use reflection to copy private properties
        $reflection = new \ReflectionClass($source);
        
        $properties = [
            '_rawData',
            '_http_code', 
            '_init',
            'query',
            'format',
            'sql',
            'meta',
            'totals',
            'extremes',
            'rows',
            'rows_before_limit_at_least',
            'array_data',
            'statistics',
            'iterator'
        ];
        
        foreach ($properties as $propertyName) {
            try {
                $property = $reflection->getProperty($propertyName);
                $value = $property->getValue($source);
                
                $thisProperty = $reflection->getProperty($propertyName);
                $thisProperty->setValue($this, $value);
            } catch (\ReflectionException $e) {
                // Skip properties that don't exist
                continue;
            }
        }
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
    /**
     * @return array
     * @throws TransportException
     */
    public function rows(): array
    {
        $this->await();
        return parent::rows();
    }

    /**
     * @param string $key
     * @return mixed|null
     * @throws TransportException
     */
    public function fetchOne($key = null)
    {
        $this->await();
        return parent::fetchOne($key);
    }

    /**
     * @param string $key
     * @return mixed|null
     * @throws TransportException
     */
    public function fetchRow($key = null)
    {
        $this->await();
        return parent::fetchRow($key);
    }

    /**
     * @return int
     * @throws TransportException
     */
    public function count(): int
    {
        $this->await();
        return parent::count();
    }

    /**
     * @return bool|int
     */
    public function countAll()
    {
        $this->await();
        return parent::countAll();
    }

    /**
     * @return array
     * @throws TransportException
     */
    public function totals(): array
    {
        $this->await();
        return parent::totals();
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremes(): array
    {
        $this->await();
        return parent::extremes();
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMin(): array
    {
        $this->await();
        return parent::extremesMin();
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMax()
    {
        $this->await();
        return parent::extremesMax();
    }

    /**
     * @param bool $key
     * @return array|mixed|null
     * @throws TransportException
     */
    public function statistics($key = false)
    {
        $this->await();
        return parent::statistics($key);
    }

    /**
     * @return array
     * @throws TransportException
     */
    public function info(): array
    {
        $this->await();
        return parent::info();
    }

    /**
     * @return array
     * @throws TransportException
     */
    public function info_upload(): array
    {
        $this->await();
        return parent::info_upload();
    }

    /**
     * @return float
     * @throws TransportException
     */
    public function totalTimeRequest(): float
    {
        $this->await();
        return parent::totalTimeRequest();
    }

    /**
     * @return mixed|string
     * @throws TransportException
     */
    public function rawData()
    {
        $this->await();
        return parent::rawData();
    }

    /**
     * @return false|string
     */
    public function jsonRows()
    {
        $this->await();
        return parent::jsonRows();
    }

    /**
     * @param string|null $path
     * @return array
     * @throws TransportException
     */
    public function rowsAsTree($path): array
    {
        $this->await();
        return parent::rowsAsTree($path);
    }

    /**
     * @return bool
     * @throws TransportException
     */
    public function isError(): bool
    {
        $this->await();
        return parent::isError();
    }

    /**
     * @throws DatabaseException
     * @throws QueryException
     */
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