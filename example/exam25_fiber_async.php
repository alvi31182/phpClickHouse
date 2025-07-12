<?php

include_once __DIR__ . '/../include.php';
include_once __DIR__ . '/Helper.php';
\ClickHouseDB\Example\Helper::init();

$config = include_once __DIR__ . '/00_config_connect.php';

echo "=== PHP ClickHouse Fiber Async Example ===\n";

// Check PHP version for Fiber support
if (PHP_VERSION_ID < 80100) {
    echo "ERROR: PHP 8.1+ required for Fiber support. Current version: " . PHP_VERSION . "\n";
    exit(1);
}

if (!class_exists('Fiber')) {
    echo "ERROR: Fiber class not available. Please enable Fiber support.\n";
    exit(1);
}

echo "PHP Version: " . PHP_VERSION . " ✓\n";
echo "Fiber Support: Available ✓\n\n";

// Create FiberClient
$client = new \ClickHouseDB\FiberClient($config);
$client->setFiberSimultaneousLimit(5); // Set concurrent limit

echo "=== Basic Async Operations ===\n";

// Start multiple async queries
$ping = $client->pingAsync();
$tables = $client->showTablesAsync();
$databases = $client->showDatabasesAsync();

echo "Started 3 async queries...\n";
echo "Pending requests: " . $client->getCountPendingQueue() . "\n";

// Execute all
$client->executeAsync();

echo "Executed all requests...\n";
echo "Active fibers: " . $client->getCountActiveFibers() . "\n";
echo "Completed requests: " . $client->getCountCompletedRequests() . "\n";

// Access results (will await automatically)
echo "Ping result: " . $ping->fetchOne('ping') . "\n";
echo "Tables count: " . count($tables->rows()) . "\n";
echo "Databases count: " . count($databases->rows()) . "\n";

echo "\n=== Batch Operations ===\n";

// Batch multiple queries
$queries = [
    ['sql' => 'SELECT 1 as query1'],
    ['sql' => 'SELECT 2 as query2'],
    ['sql' => 'SELECT 3 as query3'],
    ['sql' => 'SELECT 4 as query4'],
    ['sql' => 'SELECT 5 as query5'],
];

$statements = $client->selectBatch($queries);
echo "Started batch of " . count($statements) . " queries...\n";

// Wait for all results
$results = [];
foreach ($statements as $index => $statement) {
    $results[$index] = $statement->await()->rows();
    echo "Query " . ($index + 1) . " completed\n";
}

echo "All batch queries completed!\n";
echo "Results: " . json_encode($results) . "\n";

echo "\n=== Parallel Data Processing ===\n";

// Create test table
$client->write("DROP TABLE IF EXISTS fiber_test");
$client->write("
    CREATE TABLE fiber_test (
        id UInt32,
        name String,
        value Float64
    ) ENGINE = Memory
");

// Insert test data
$testData = [];
for ($i = 1; $i <= 100; $i++) {
    $testData[] = [$i, "item_$i", $i * 1.5];
}
$client->insert('fiber_test', $testData, ['id', 'name', 'value']);

echo "Created test table with 100 rows\n";

// Parallel queries on the same table
$queries = [
    ['sql' => 'SELECT count() as total FROM fiber_test'],
    ['sql' => 'SELECT avg(value) as avg_value FROM fiber_test'],
    ['sql' => 'SELECT max(value) as max_value FROM fiber_test'],
    ['sql' => 'SELECT min(value) as min_value FROM fiber_test'],
    ['sql' => 'SELECT sum(value) as sum_value FROM fiber_test'],
];

$statements = $client->selectBatch($queries);
echo "Started 5 parallel aggregation queries...\n";

// Process results as they become available
$results = [];
foreach ($statements as $index => $statement) {
    $result = $statement->await()->rows();
    $results[$index] = $result[0];
    echo "Query " . ($index + 1) . " result: " . json_encode($result[0]) . "\n";
}

echo "\n=== Performance Comparison ===\n";

// Test synchronous vs asynchronous performance
$iterations = 10;
$queries = [];
for ($i = 0; $i < $iterations; $i++) {
    $queries[] = ['sql' => "SELECT $i as iteration, sleep(0.1) as delay"];
}

// Synchronous execution
$start = microtime(true);
foreach ($queries as $query) {
    $client->select($query['sql']);
}
$syncTime = microtime(true) - $start;

// Asynchronous execution
$start = microtime(true);
$statements = $client->selectBatch($queries);
foreach ($statements as $statement) {
    $statement->await();
}
$asyncTime = microtime(true) - $start;

echo "Synchronous execution time: " . round($syncTime, 3) . "s\n";
echo "Asynchronous execution time: " . round($asyncTime, 3) . "s\n";
echo "Speedup: " . round($syncTime / $asyncTime, 2) . "x\n";

echo "\n=== Error Handling ===\n";

// Test error handling with async operations
try {
    $errorQuery = $client->selectAsync('SELECT * FROM non_existent_table');
    $client->executeAsync();
    
    // This should throw an exception
    $errorQuery->await()->rows();
} catch (\Exception $e) {
    echo "Error caught: " . $e->getMessage() . "\n";
}

echo "\n=== Fiber Info ===\n";
echo "Fiber handler info: " . $client->getFiberInfo() . "\n";
echo "Simultaneous limit: " . $client->getFiberSimultaneousLimit() . "\n";

echo "\n=== Cleanup ===\n";
$client->write("DROP TABLE IF EXISTS fiber_test");
echo "Test table dropped\n";

echo "\n=== Example Completed Successfully! ===\n";
echo "Fiber-based async operations work correctly.\n";
echo "Benefits:\n";
echo "- True async/await pattern\n";
echo "- Better resource utilization\n";
echo "- Improved performance for parallel operations\n";
echo "- Cooperative multitasking\n"; 