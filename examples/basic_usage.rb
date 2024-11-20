require_relative '../lib/order_book'
require 'connection_pool'
require 'redis'

# 创建Redis连接池
redis_pool = ConnectionPool.new(size: 5) { Redis.new(url: 'redis://localhost:6379/0') }

# 创建BTC/USDT交易对的订单簿
order_book = ExchangeEngine::OrderBook.new('BTCUSDT', redis_pool)

# 创建一些测试订单
buy_order = {
  id: "order_#{Time.now.to_i}_1",
  side: 'buy',
  price: 30000.0,
  amount: 1.5,
  timestamp: Time.now.to_i
}

sell_order = {
  id: "order_#{Time.now.to_i}_2",
  side: 'sell',
  price: 30000.0,
  amount: 1.0,
  timestamp: Time.now.to_i
}

# 添加订单
puts "Adding buy order..."
order_book.add_limit_order(buy_order)

puts "Adding sell order..."
order_book.add_limit_order(sell_order)

# 订单会自动撮合，因为价格相同
puts "Orders should be matched automatically"
