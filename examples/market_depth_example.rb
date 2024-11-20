require_relative '../lib/order_processor'
require 'connection_pool'
require 'redis'
require 'oj'

# 初始化 Redis 连接池
redis_pool = ConnectionPool.new(size: 5) { Redis.new(url: ENV['REDIS_URL'] || 'redis://localhost:6379/0') }

# 创建订单处理器
processor = ExchangeEngine::OrderProcessor.new(redis_pool, 'BTCUSDT')
Thread.new {processor.start}

# 创建一些测试订单
orders = [
  # 买单
  {
    'id' => 'bid1',
    'type' => 'limit',
    'side' => 'buy',
    'price' => '50000.00',
    'amount' => '1.0',
    'trading_pair' => 'BTCUSDT'
  },
  {
    'id' => 'bid2',
    'type' => 'limit',
    'side' => 'buy',
    'price' => '49900.00',
    'amount' => '2.0',
    'trading_pair' => 'BTCUSDT'
  },
  # 卖单
  {
    'id' => 'ask1',
    'type' => 'limit',
    'side' => 'sell',
    'price' => '50100.00',
    'amount' => '1.5',
    'trading_pair' => 'BTCUSDT'
  },
  {
    'id' => 'ask2',
    'type' => 'limit',
    'side' => 'sell',
    'price' => '50200.00',
    'amount' => '2.5',
    'trading_pair' => 'BTCUSDT'
  }
]

# 提交订单
orders.each do |order|
  redis_pool.with do |conn|
    conn.lpush("pending_orders:BTCUSDT", Oj.dump(order))
  end
end

# 等待订单处理
sleep(1)

# 获取市场深度
depth = processor.instance_variable_get(:@order_book).get_market_depth(10)

puts "\nMarket Depth for BTCUSDT:"
puts "\nBids (Buy Orders):"
puts "Price\t\tAmount\t\tTotal"
depth['bids'].each do |level|
  puts "#{level['price']}\t#{level['amount']}\t#{level['total']}"
end

puts "\nAsks (Sell Orders):"
puts "Price\t\tAmount\t\tTotal"
depth['asks'].each do |level|
  puts "#{level['price']}\t#{level['amount']}\t#{level['total']}"
end

# 停止处理器
processor.stop
