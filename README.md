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
