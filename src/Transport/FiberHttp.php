<?php

namespace ClickHouseDB\Transport;

use ClickHouseDB\Exception\TransportException;
use ClickHouseDB\Query\Degeneration;
use ClickHouseDB\Query\Query;
use ClickHouseDB\Query\WhereInFile;
use ClickHouseDB\Query\WriteToFile;
use ClickHouseDB\Settings;
use ClickHouseDB\Statement\FiberStatement;
use ClickHouseDB\Statement;

/**
 * HTTP transport implementation using PHP Fibers for true async operations
 * Extends Http class to provide fiber-based async functionality
 */
class FiberHttp extends Http
{
    /**
     * @var FiberHandler
     */
    private $fiberHandler;

    /**
     * @var array<string, string>
     */
    private $requestIdMap = [];

    public function __construct($host, $port, $username, $password, $authMethod = null)
    {
        parent::__construct($host, $port, $username, $password, $authMethod);
        $this->fiberHandler = new FiberHandler(10); // Default limit
    }

    /**
     * Set the fiber handler
     */
    public function setFiberHandler(FiberHandler $handler): self
    {
        $this->fiberHandler = $handler;
        return $this;
    }

    /**
     * Get the fiber handler
     */
    public function getFiberHandler(): FiberHandler
    {
        return $this->fiberHandler;
    }

    /**
     * Set simultaneous limit for fibers
     */
    public function setFiberSimultaneousLimit(int $limit): self
    {
        $this->fiberHandler->setSimultaneousLimit($limit);
        return $this;
    }

    /**
     * Get simultaneous limit for fibers
     */
    public function getFiberSimultaneousLimit(): int
    {
        return $this->fiberHandler->getSimultaneousLimit();
    }

    /**
     * Async select using fibers
     */
    public function selectAsync($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): FiberStatement
    {
        $request = $this->prepareSelect($sql, $bindings, $whereInFile, $writeToFile);
        $requestId = $this->fiberHandler->addRequest($request);
        
        // Store mapping for later reference
        $this->requestIdMap[$requestId] = $requestId;
        
        return new FiberStatement($request, $this->fiberHandler, $requestId);
    }

    /**
     * Execute all pending async requests
     */
    public function executeAsync(): bool
    {
        $this->fiberHandler->executeAsync();
        return true;
    }

    /**
     * Get count of pending requests
     */
    public function getCountPendingQueue(): int
    {
        return $this->fiberHandler->countPending();
    }

    /**
     * Get count of active fibers
     */
    public function getCountActiveFibers(): int
    {
        return $this->fiberHandler->countActive();
    }

    /**
     * Get count of completed requests
     */
    public function getCountCompletedRequests(): int
    {
        return $this->fiberHandler->countCompleted();
    }

    /**
     * Clear all pending requests
     */
    public function clearPendingRequests(): void
    {
        $this->fiberHandler->clear();
        $this->requestIdMap = [];
    }

    /**
     * Get info about fiber handler state
     */
    public function getFiberInfo(): string
    {
        return $this->fiberHandler->getInfo();
    }

    /**
     * Wait for specific request to complete
     */
    public function waitForRequest(string $requestId): ?CurlerRequest
    {
        while (!$this->fiberHandler->isRequestCompleted($requestId)) {
            usleep(1000); // 1ms delay
        }
        
        return $this->fiberHandler->getCompletedRequest($requestId);
    }

    /**
     * Wait for all pending requests to complete
     */
    public function waitForAllRequests(): array
    {
        $this->fiberHandler->executeAsync();
        return $this->fiberHandler->getCompletedRequests();
    }

    /**
     * Check if specific request is completed
     */
    public function isRequestCompleted(string $requestId): bool
    {
        return $this->fiberHandler->isRequestCompleted($requestId);
    }

    /**
     * Get completed request by ID
     */
    public function getCompletedRequest(string $requestId): ?CurlerRequest
    {
        return $this->fiberHandler->getCompletedRequest($requestId);
    }

    /**
     * Get all completed requests
     */
    public function getCompletedRequests(): array
    {
        return $this->fiberHandler->getCompletedRequests();
    }

    /**
     * Override parent select to use fibers internally
     */
    public function select($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Statement
    {
        $fiberStatement = $this->selectAsync($sql, $bindings, $whereInFile, $writeToFile);
        $this->executeAsync();
        return $fiberStatement->await();
    }

    /**
     * Override parent write to use fibers internally
     */
    public function write($sql, array $bindings = [], $exception = true): Statement
    {
        $request = $this->prepareWrite($sql, $bindings);
        $requestId = $this->fiberHandler->addRequest($request);
        
        $this->fiberHandler->executeAsync();
        
        $completedRequest = $this->fiberHandler->getCompletedRequest($requestId);
        if (!$completedRequest) {
            throw new TransportException("Failed to execute write request");
        }
        
        return new Statement($completedRequest);
    }

    /**
     * Batch select multiple queries asynchronously
     */
    public function selectBatch(array $queries): array
    {
        $statements = [];
        
        // Add all queries to fiber handler
        foreach ($queries as $index => $query) {
            $sql = $query['sql'] ?? '';
            $bindings = $query['bindings'] ?? [];
            $whereInFile = $query['whereInFile'] ?? null;
            $writeToFile = $query['writeToFile'] ?? null;
            
            $statements[$index] = $this->selectAsync($sql, $bindings, $whereInFile, $writeToFile);
        }
        
        // Execute all
        $this->executeAsync();
        
        // Return statements that will await when accessed
        return $statements;
    }

    /**
     * Execute multiple queries in parallel and wait for all results
     */
    public function executeBatch(array $queries): array
    {
        $statements = $this->selectBatch($queries);
        
        // Wait for all results
        $results = [];
        foreach ($statements as $index => $statement) {
            $results[$index] = $statement->await()->rows();
        }
        
        return $results;
    }

    /**
     * Get request ID mapping
     */
    public function getRequestIdMap(): array
    {
        return $this->requestIdMap;
    }

    /**
     * Clean up resources
     */
    public function __destruct()
    {
        $this->clearPendingRequests();
    }
} 