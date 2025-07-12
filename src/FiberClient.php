<?php

namespace ClickHouseDB;

use ClickHouseDB\Query\WhereInFile;
use ClickHouseDB\Query\WriteToFile;
use ClickHouseDB\Statement\FiberStatement;
use ClickHouseDB\Transport\FiberHttp;

/**
 * Async ClickHouse client using PHP Fibers
 * Provides true async/await pattern for all operations
 */
class FiberClient extends Client
{
    /**
     * @var FiberHttp
     */
    private $fiberTransport;

    public function __construct(array $connectParams, array $settings = [])
    {
        parent::__construct($connectParams, $settings);
        
        // Replace the transport with FiberHttp
        $this->fiberTransport = new FiberHttp(
            $this->getConnectHost(),
            $this->getConnectPort(),
            $this->getConnectUsername(),
            $this->getConnectPassword(),
            $this->getAuthMethod()
        );
        
        // Copy settings from parent transport
        $this->fiberTransport->settings()->apply($this->settings()->getSettings());
    }

    /**
     * Get the fiber transport
     */
    public function fiberTransport(): FiberHttp
    {
        return $this->fiberTransport;
    }

    /**
     * Set fiber simultaneous limit
     */
    public function setFiberSimultaneousLimit(int $limit): self
    {
        $this->fiberTransport->setFiberSimultaneousLimit($limit);
        return $this;
    }

    /**
     * Get fiber simultaneous limit
     */
    public function getFiberSimultaneousLimit(): int
    {
        return $this->fiberTransport->getFiberSimultaneousLimit();
    }

    /**
     * Async select using fibers
     */
    public function selectAsync(
        string $sql,
        array $bindings = [],
        ?WhereInFile $whereInFile = null,
        ?WriteToFile $writeToFile = null
    ): FiberStatement {
        return $this->fiberTransport->selectAsync($sql, $bindings, $whereInFile, $writeToFile);
    }

    /**
     * Execute all pending async requests
     */
    public function executeAsync(): bool
    {
        return $this->fiberTransport->executeAsync();
    }

    /**
     * Get count of pending requests
     */
    public function getCountPendingQueue(): int
    {
        return $this->fiberTransport->getCountPendingQueue();
    }

    /**
     * Get count of active fibers
     */
    public function getCountActiveFibers(): int
    {
        return $this->fiberTransport->getCountActiveFibers();
    }

    /**
     * Get count of completed requests
     */
    public function getCountCompletedRequests(): int
    {
        return $this->fiberTransport->getCountCompletedRequests();
    }

    /**
     * Clear all pending requests
     */
    public function clearPendingRequests(): void
    {
        $this->fiberTransport->clearPendingRequests();
    }

    /**
     * Get fiber handler info
     */
    public function getFiberInfo(): string
    {
        return $this->fiberTransport->getFiberInfo();
    }

    /**
     * Batch select multiple queries asynchronously
     */
    public function selectBatch(array $queries): array
    {
        return $this->fiberTransport->selectBatch($queries);
    }

    /**
     * Execute multiple queries in parallel and wait for all results
     */
    public function executeBatch(array $queries): array
    {
        return $this->fiberTransport->executeBatch($queries);
    }

    /**
     * Wait for specific request to complete
     */
    public function waitForRequest(string $requestId): ?\ClickHouseDB\Transport\CurlerRequest
    {
        return $this->fiberTransport->waitForRequest($requestId);
    }

    /**
     * Wait for all pending requests to complete
     */
    public function waitForAllRequests(): array
    {
        return $this->fiberTransport->waitForAllRequests();
    }

    /**
     * Check if specific request is completed
     */
    public function isRequestCompleted(string $requestId): bool
    {
        return $this->fiberTransport->isRequestCompleted($requestId);
    }

    /**
     * Get completed request by ID
     */
    public function getCompletedRequest(string $requestId): ?\ClickHouseDB\Transport\CurlerRequest
    {
        return $this->fiberTransport->getCompletedRequest($requestId);
    }

    /**
     * Get all completed requests
     */
    public function getCompletedRequests(): array
    {
        return $this->fiberTransport->getCompletedRequests();
    }

    /**
     * Override parent methods to use fiber transport
     */
    public function select(
        string $sql,
        array $bindings = [],
        ?WhereInFile $whereInFile = null,
        ?WriteToFile $writeToFile = null
    ) {
        return $this->fiberTransport->select($sql, $bindings, $whereInFile, $writeToFile);
    }

    public function write(string $sql, array $bindings = [], bool $exception = true)
    {
        return $this->fiberTransport->write($sql, $bindings, $exception);
    }

    public function transport(): FiberHttp
    {
        return $this->fiberTransport;
    }

    /**
     * Async ping using fibers
     */
    public function pingAsync(): FiberStatement
    {
        return $this->selectAsync('SELECT 1 as ping');
    }

    /**
     * Async show tables using fibers
     */
    public function showTablesAsync(): FiberStatement
    {
        return $this->selectAsync('SHOW TABLES');
    }

    /**
     * Async show databases using fibers
     */
    public function showDatabasesAsync(): FiberStatement
    {
        return $this->selectAsync('SHOW DATABASES');
    }

    /**
     * Async show create table using fibers
     */
    public function showCreateTableAsync(string $table): FiberStatement
    {
        return $this->selectAsync("SHOW CREATE TABLE $table");
    }

    /**
     * Async table size using fibers
     */
    public function tableSizeAsync(string $tableName): FiberStatement
    {
        return $this->selectAsync("SELECT sum(bytes) as size FROM system.parts WHERE table = '$tableName'");
    }

    /**
     * Async database size using fibers
     */
    public function databaseSizeAsync(): FiberStatement
    {
        return $this->selectAsync('SELECT sum(bytes) as size FROM system.parts');
    }

    /**
     * Async partitions using fibers
     */
    public function partitionsAsync(string $table, int $limit = 0, ?bool $active = null): FiberStatement
    {
        $sql = "SELECT * FROM system.parts WHERE table = '$table'";
        if ($active !== null) {
            $sql .= " AND active = " . ($active ? '1' : '0');
        }
        if ($limit > 0) {
            $sql .= " LIMIT $limit";
        }
        
        return $this->selectAsync($sql);
    }

    /**
     * Async insert using fibers
     */
    public function insertAsync(string $table, array $values, array $columns = []): FiberStatement
    {
        $sql = "INSERT INTO $table";
        if (!empty($columns)) {
            $sql .= " (" . implode(', ', $columns) . ")";
        }
        $sql .= " VALUES";
        
        $placeholders = [];
        foreach ($values as $row) {
            $rowPlaceholders = [];
            foreach ($row as $value) {
                $rowPlaceholders[] = is_string($value) ? "'$value'" : $value;
            }
            $placeholders[] = '(' . implode(', ', $rowPlaceholders) . ')';
        }
        
        $sql .= ' ' . implode(', ', $placeholders);
        
        return $this->selectAsync($sql);
    }

    /**
     * Example usage method showing async pattern
     */
    public function exampleAsyncUsage(): void
    {
        // Start multiple async queries
        $ping = $this->pingAsync();
        $tables = $this->showTablesAsync();
        $databases = $this->showDatabasesAsync();
        
        // Execute all
        $this->executeAsync();
        
        // Access results when needed (will await automatically)
        echo "Ping result: " . $ping->fetchOne('ping') . "\n";
        echo "Tables: " . json_encode($tables->rows()) . "\n";
        echo "Databases: " . json_encode($databases->rows()) . "\n";
    }
} 