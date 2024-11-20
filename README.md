# Exchange Engine

A high-performance trading system with Redis-backed order book management and a Sinatra-based HTTP API.

## Features

- Fast order matching engine using Redis
- Support for limit orders
- Real-time order book management
- REST API for order operations
- Price-time priority matching
- Detailed logging and monitoring
- Trade history queue
- Asynchronous order processing

## Prerequisites

- Ruby 3.0+
- Redis 5.0+

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd exchange_engine
```

2. Install dependencies:
```bash
bundle install
```

3. Configure Redis connection in `config/config.yml`:
```yaml
development:
  redis_url: redis://localhost:6379/0
  port: 3000
  trading_pairs:
    - BTCUSDT
    - ETHUSDT
  log_level: info
```

## Redis Configuration

### AOF (Append Only File) Mode

For a trading system, data durability is crucial. We strongly recommend using Redis AOF mode to ensure no trades are lost in case of system crashes or power failures.

1. Edit your `redis.conf`:
```conf
# Enable AOF
appendonly yes

# AOF fsync policy options:
# - always: fsync after every write (safest, slowest)
# - everysec: fsync every second (good compromise)
# - no: let OS handle fsync (fastest, least safe)
appendfsync everysec

# Auto-rewrite AOF file when it gets too large
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# If you have enough memory, you can also enable RDB snapshots alongside AOF
save 900 1
save 300 10
save 60 10000
```

2. Verify AOF is working:
```bash
redis-cli info persistence
```

Look for:
```
aof_enabled:1
aof_rewrite_in_progress:0
aof_last_rewrite_time_sec:-1
aof_current_size:0
aof_base_size:0
```

3. Monitor AOF status:
```bash
# Check AOF file size
ls -lh /path/to/redis/appendonly.aof

# Check AOF rewrite status
redis-cli info persistence | grep aof_
```

4. Recovery process:
```bash
# If Redis fails to start due to corrupted AOF
redis-check-aof --fix /path/to/redis/appendonly.aof
```

### Performance Considerations

1. **fsync Policy**:
   - `appendfsync always`: Maximum safety but slower performance
   - `appendfsync everysec`: Good compromise (recommended)
   - `appendfsync no`: Maximum performance but risk of data loss

2. **AOF Rewrite**:
   - Automatically triggered when AOF size grows by 100%
   - Minimum size threshold: 64MB
   - Adjust these values based on your system capacity

3. **Combined Persistence**:
   - Consider enabling both AOF and RDB for better recovery options
   - RDB provides point-in-time snapshots
   - AOF ensures command-by-command durability

## Usage

### Starting the Server

```bash
./bin/exchange_engine
```

### API Endpoints

#### Create Order
```bash
POST /api/v1/orders
Content-Type: application/json

{
  "trading_pair": "BTCUSDT",
  "side": "buy",
  "price": 42000.0,
  "amount": 1.5
}
```

#### Cancel Order
```bash
DELETE /api/v1/orders/:order_id?trading_pair=BTCUSDT
```

#### Query Order
```bash
GET /api/v1/orders/:order_id?trading_pair=BTCUSDT
```

#### Health Check
```bash
GET /health
```

### Example Code

```ruby
require_relative 'lib/order_book'

# Initialize Redis connection pool
redis_pool = ConnectionPool.new(size: 5) do
  Redis.new(url: 'redis://localhost:6379/0')
end

# Create order book instance
order_book = ExchangeEngine::OrderBook.new('BTCUSDT', redis_pool)

# Add buy order
order_book.add_limit_order({
  id: "order_#{Time.now.to_i}",
  side: 'buy',
  price: 42000.0,
  amount: 1.5
})

# Print order book status
order_book.print_order_book

# View recent trades
order_book.print_recent_trades
```

## Testing

Run the test suite:
```bash
bundle exec rspec
```

## Architecture

- Core matching engine using Redis sorted sets for price levels
- Connection pooling for Redis connections
- Atomic operations for order matching
- Trade history using Redis lists
- RESTful API with JSON responses

## Performance Considerations

- Uses Redis sorted sets for efficient price level management
- Connection pooling to handle concurrent requests
- Atomic operations to ensure data consistency
- Capped trade history queue

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License
