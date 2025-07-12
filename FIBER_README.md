# PHP ClickHouse Fiber Async Support

Этот модуль добавляет поддержку PHP Fiber для истинно асинхронных операций с ClickHouse.

## Требования

- PHP 8.1+ (для поддержки Fiber)
- ext-curl
- ext-json

## Архитектура

### Компоненты

1. **FiberHandler** - управляет пулом Fiber для асинхронных запросов
2. **FiberHttp** - HTTP транспорт, использующий FiberHandler
3. **FiberClient** - основной клиент для асинхронных операций
4. **FiberStatement** - асинхронный Statement с поддержкой await

### Преимущества перед curl_multi

- **Истинный async/await паттерн** - использует PHP Fiber вместо curl_multi
- **Лучшая производительность** - кооперативная многозадачность
- **Простота использования** - интуитивный API
- **Лучшее управление ресурсами** - автоматическое управление памятью

## Использование

### Базовые асинхронные операции

```php
use ClickHouseDB\FiberClient;

$config = [
    'host' => '127.0.0.1',
    'port' => '8123',
    'username' => 'default',
    'password' => ''
];

$client = new FiberClient($config);
$client->setFiberSimultaneousLimit(10); // Лимит одновременных запросов

// Запуск асинхронных запросов
$ping = $client->pingAsync();
$tables = $client->showTablesAsync();
$databases = $client->showDatabasesAsync();

// Выполнение всех запросов
$client->executeAsync();

// Получение результатов (автоматический await)
echo $ping->fetchOne('ping'); // 1
echo count($tables->rows()); // количество таблиц
echo count($databases->rows()); // количество баз данных
```

### Пакетные операции

```php
// Пакетный запрос
$queries = [
    ['sql' => 'SELECT count() FROM table1'],
    ['sql' => 'SELECT count() FROM table2'],
    ['sql' => 'SELECT count() FROM table3'],
];

$statements = $client->selectBatch($queries);

// Ожидание всех результатов
foreach ($statements as $statement) {
    $result = $statement->await()->rows();
    echo json_encode($result) . "\n";
}
```

### Параллельная обработка данных

```php
// Создание тестовой таблицы
$client->write("
    CREATE TABLE test_table (
        id UInt32,
        name String,
        value Float64
    ) ENGINE = Memory
");

// Параллельные агрегации
$queries = [
    ['sql' => 'SELECT count() as total FROM test_table'],
    ['sql' => 'SELECT avg(value) as avg_value FROM test_table'],
    ['sql' => 'SELECT max(value) as max_value FROM test_table'],
    ['sql' => 'SELECT min(value) as min_value FROM test_table'],
];

$statements = $client->selectBatch($queries);

// Обработка результатов по мере готовности
foreach ($statements as $index => $statement) {
    $result = $statement->await()->rows();
    echo "Query $index: " . json_encode($result[0]) . "\n";
}
```

### Сравнение производительности

```php
$iterations = 10;
$queries = [];
for ($i = 0; $i < $iterations; $i++) {
    $queries[] = ['sql' => "SELECT $i as iteration, sleep(0.1) as delay"];
}

// Синхронное выполнение
$start = microtime(true);
foreach ($queries as $query) {
    $client->select($query['sql']);
}
$syncTime = microtime(true) - $start;

// Асинхронное выполнение
$start = microtime(true);
$statements = $client->selectBatch($queries);
foreach ($statements as $statement) {
    $statement->await();
}
$asyncTime = microtime(true) - $start;

echo "Синхронно: " . round($syncTime, 3) . "s\n";
echo "Асинхронно: " . round($asyncTime, 3) . "s\n";
echo "Ускорение: " . round($syncTime / $asyncTime, 2) . "x\n";
```

## API Reference

### FiberClient

#### Основные методы

- `selectAsync(string $sql, array $bindings = [])` - асинхронный SELECT
- `executeAsync()` - выполнение всех ожидающих запросов
- `selectBatch(array $queries)` - пакетный запрос
- `executeBatch(array $queries)` - выполнение пакета с ожиданием результатов

#### Настройки

- `setFiberSimultaneousLimit(int $limit)` - установка лимита одновременных запросов
- `getFiberSimultaneousLimit()` - получение текущего лимита

#### Мониторинг

- `getCountPendingQueue()` - количество ожидающих запросов
- `getCountActiveFibers()` - количество активных Fiber
- `getCountCompletedRequests()` - количество завершенных запросов
- `getFiberInfo()` - информация о состоянии FiberHandler

### FiberStatement

#### Методы

- `await()` - ожидание результата
- `isReady()` - проверка готовности результата
- Все методы родительского Statement (автоматически вызывают await)

## Миграция с обычного Client

```php
// Старый код
$client = new ClickHouseDB\Client($config);
$state1 = $client->selectAsync('SELECT 1');
$state2 = $client->selectAsync('SELECT 2');
$client->executeAsync();
$result1 = $state1->rows();
$result2 = $state2->rows();

// Новый код с Fiber
$client = new ClickHouseDB\FiberClient($config);
$state1 = $client->selectAsync('SELECT 1');
$state2 = $client->selectAsync('SELECT 2');
$client->executeAsync();
$result1 = $state1->await()->rows(); // или просто $state1->rows()
$result2 = $state2->await()->rows(); // или просто $state2->rows()
```

## Обработка ошибок

```php
try {
    $query = $client->selectAsync('SELECT * FROM non_existent_table');
    $client->executeAsync();
    
    // Исключение будет выброшено при обращении к результату
    $result = $query->await()->rows();
} catch (\ClickHouseDB\Exception\DatabaseException $e) {
    echo "Database error: " . $e->getMessage() . "\n";
} catch (\Exception $e) {
    echo "General error: " . $e->getMessage() . "\n";
}
```

## Производительность

### Преимущества Fiber

1. **Кооперативная многозадачность** - более эффективное использование CPU
2. **Меньше накладных расходов** - нет необходимости в curl_multi
3. **Лучшее управление памятью** - автоматическое освобождение ресурсов
4. **Простота отладки** - синхронный код в асинхронном контексте

### Рекомендации

- Устанавливайте `simultaneousLimit` в зависимости от возможностей сервера
- Используйте пакетные операции для множественных запросов
- Обрабатывайте ошибки для каждого запроса отдельно
- Мониторьте использование памяти при большом количестве одновременных запросов

## Примеры

Смотрите `example/exam25_fiber_async.php` для полного примера использования.

## Совместимость

- Полная совместимость с существующим API
- Обратная совместимость с обычным Client
- Поддержка всех существующих функций (bindings, conditions, etc.) 