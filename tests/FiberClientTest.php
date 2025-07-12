<?php

namespace ClickHouseDB\Tests;

use ClickHouseDB\FiberClient;
use PHPUnit\Framework\TestCase;

/**
 * @group FiberClient
 * @requires PHP 8.1
 */
class FiberClientTest extends TestCase
{
    use WithClient;

    private FiberClient $fiberClient;

    protected function setUp(): void
    {
        parent::setUp();
        
        if (PHP_VERSION_ID < 80100) {
            $this->markTestSkipped('PHP 8.1+ required for Fiber support');
        }
        
        if (!class_exists('Fiber')) {
            $this->markTestSkipped('Fiber class not available');
        }
        
        $this->fiberClient = new FiberClient($this->getConfig());
        $this->fiberClient->setFiberSimultaneousLimit(5);
    }

    private function getConfig(): array
    {
        return [
            'host' => getenv('CLICKHOUSE_HOST') ?: '127.0.0.1',
            'port' => getenv('CLICKHOUSE_PORT') ?: '8123',
            'username' => getenv('CLICKHOUSE_USER') ?: 'default',
            'password' => getenv('CLICKHOUSE_PASSWORD') ?: '',
            'database' => getenv('CLICKHOUSE_DATABASE') ?: 'default',
        ];
    }

    public function testBasicAsyncOperations()
    {
        // Start async queries
        $ping = $this->fiberClient->pingAsync();
        $tables = $this->fiberClient->showTablesAsync();
        $databases = $this->fiberClient->showDatabasesAsync();

        // Execute all
        $this->fiberClient->executeAsync();

        // Check results
        $this->assertEquals(1, $ping->fetchOne('ping'));
        $this->assertIsArray($tables->rows());
        $this->assertIsArray($databases->rows());
    }

    public function testBatchOperations()
    {
        $queries = [
            ['sql' => 'SELECT 1 as query1'],
            ['sql' => 'SELECT 2 as query2'],
            ['sql' => 'SELECT 3 as query3'],
        ];

        $statements = $this->fiberClient->selectBatch($queries);

        $this->assertCount(3, $statements);

        // Wait for all results
        $results = [];
        foreach ($statements as $index => $statement) {
            $results[$index] = $statement->await()->rows();
        }

        $this->assertEquals(1, $results[0][0]['query1']);
        $this->assertEquals(2, $results[1][0]['query2']);
        $this->assertEquals(3, $results[2][0]['query3']);
    }

    public function testParallelDataProcessing()
    {
        // Create test table
        $this->fiberClient->write("DROP TABLE IF EXISTS fiber_test");
        $this->fiberClient->write("
            CREATE TABLE fiber_test (
                id UInt32,
                name String,
                value Float64
            ) ENGINE = Memory
        ");

        // Insert test data
        $testData = [];
        for ($i = 1; $i <= 10; $i++) {
            $testData[] = [$i, "item_$i", $i * 1.5];
        }
        $this->fiberClient->insert('fiber_test', $testData, ['id', 'name', 'value']);

        // Parallel queries
        $queries = [
            ['sql' => 'SELECT count() as total FROM fiber_test'],
            ['sql' => 'SELECT avg(value) as avg_value FROM fiber_test'],
            ['sql' => 'SELECT max(value) as max_value FROM fiber_test'],
        ];

        $statements = $this->fiberClient->selectBatch($queries);

        // Process results
        $results = [];
        foreach ($statements as $index => $statement) {
            $results[$index] = $statement->await()->rows()[0];
        }

        $this->assertEquals(10, $results[0]['total']);
        $this->assertEquals(8.25, $results[1]['avg_value']);
        $this->assertEquals(15.0, $results[2]['max_value']);

        // Cleanup
        $this->fiberClient->write("DROP TABLE IF EXISTS fiber_test");
    }

    public function testErrorHandling()
    {
        $this->expectException(\ClickHouseDB\Exception\DatabaseException::class);

        $query = $this->fiberClient->selectAsync('SELECT * FROM non_existent_table');
        $this->fiberClient->executeAsync();
        
        // This should throw an exception
        $query->await()->rows();
    }

    public function testFiberInfo()
    {
        $info = $this->fiberClient->getFiberInfo();
        $this->assertIsString($info);
        $this->assertStringContainsString('activeFibers', $info);
    }

    public function testSimultaneousLimit()
    {
        $this->fiberClient->setFiberSimultaneousLimit(15);
        $this->assertEquals(15, $this->fiberClient->getFiberSimultaneousLimit());
    }

    public function testRequestCounts()
    {
        $this->assertEquals(0, $this->fiberClient->getCountPendingQueue());
        $this->assertEquals(0, $this->fiberClient->getCountActiveFibers());
        $this->assertEquals(0, $this->fiberClient->getCountCompletedRequests());

        // Start a query
        $ping = $this->fiberClient->pingAsync();
        $this->assertEquals(1, $this->fiberClient->getCountPendingQueue());

        // Execute
        $this->fiberClient->executeAsync();
        $this->assertEquals(0, $this->fiberClient->getCountPendingQueue());
        $this->assertEquals(1, $this->fiberClient->getCountCompletedRequests());

        // Access result
        $ping->await();
        $this->assertEquals(1, $this->fiberClient->getCountCompletedRequests());
    }

    public function testClearPendingRequests()
    {
        $ping = $this->fiberClient->pingAsync();
        $this->assertEquals(1, $this->fiberClient->getCountPendingQueue());

        $this->fiberClient->clearPendingRequests();
        $this->assertEquals(0, $this->fiberClient->getCountPendingQueue());
    }

    public function testAsyncMethods()
    {
        $ping = $this->fiberClient->pingAsync();
        $tables = $this->fiberClient->showTablesAsync();
        $databases = $this->fiberClient->showDatabasesAsync();

        $this->fiberClient->executeAsync();

        $this->assertEquals(1, $ping->fetchOne('ping'));
        $this->assertIsArray($tables->rows());
        $this->assertIsArray($databases->rows());
    }

    public function testIsReady()
    {
        $ping = $this->fiberClient->pingAsync();
        
        // Should not be ready before execution
        $this->assertFalse($ping->isReady());
        
        $this->fiberClient->executeAsync();
        
        // Should be ready after execution
        $this->assertTrue($ping->isReady());
    }

    public function testRequestId()
    {
        $ping = $this->fiberClient->pingAsync();
        $this->assertIsString($ping->getRequestId());
        $this->assertNotEmpty($ping->getRequestId());
    }

    public function testPerformanceComparison()
    {
        $iterations = 5;
        $queries = [];
        for ($i = 0; $i < $iterations; $i++) {
            $queries[] = ['sql' => "SELECT $i as iteration"];
        }

        // Synchronous execution
        $start = microtime(true);
        foreach ($queries as $query) {
            $this->fiberClient->select($query['sql']);
        }
        $syncTime = microtime(true) - $start;

        // Asynchronous execution
        $start = microtime(true);
        $statements = $this->fiberClient->selectBatch($queries);
        foreach ($statements as $statement) {
            $statement->await();
        }
        $asyncTime = microtime(true) - $start;

        // Async should be faster for multiple queries
        $this->assertLessThanOrEqual($syncTime, $asyncTime);
    }
} 