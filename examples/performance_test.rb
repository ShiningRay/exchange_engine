require 'redis'
require 'connection_pool'
require 'oj'
require 'parallel'
require 'securerandom'
require 'bigdecimal'
require_relative '../lib/performance_monitor'

module ExchangeEngine
  class PerformanceTest
    def initialize(redis_pool, trading_pair)
      @redis = redis_pool
      @trading_pair = trading_pair
      @monitor = PerformanceMonitor.new(redis_pool)
    end

    def run_test(num_orders: 1000, num_threads: 4, batch_size: 100)
      puts "Starting performance test for #{@trading_pair}..."
      puts "Orders: #{num_orders}, Threads: #{num_threads}, Batch size: #{batch_size}"

      start_time = Time.now
      
      # 生成测试订单
      orders = generate_test_orders(num_orders)
      
      # 分批提交订单
      Parallel.each(orders.each_slice(batch_size).to_a, in_threads: num_threads) do |batch|
        submit_orders(batch)
      end

      duration = Time.now - start_time
      throughput = num_orders / duration

      puts "\nTest completed for #{@trading_pair}:"
      puts "Total time: #{duration.round(2)} seconds"
      puts "Throughput: #{throughput.round(2)} orders/second"

      print_metrics
    end

    private

    def generate_test_orders(count)
      orders = []
      count.times do |i|
        # 生成随机价格和数量，确保合理的范围
        price = case @trading_pair
        when 'BTCUSDT'
          rand(30000..50000)
        when 'ETHUSDT'
          rand(2000..4000)
        else
          rand(1..1000)
        end

        amount = rand(0.1..2.0)
        
        # 使用 BigDecimal 确保精确计算
        price_bd = BigDecimal(price.to_s)
        amount_bd = BigDecimal(amount.round(8).to_s)

        orders << {
          'id' => SecureRandom.uuid,
          'trading_pair' => @trading_pair,
          'type' => 'limit',
          'side' => i.even? ? 'buy' : 'sell',
          'price' => price_bd.to_s('F'),
          'amount' => amount_bd.to_s('F'),
          'timestamp' => Time.now.to_i
        }
      end
      orders
    end

    def submit_orders(orders)
      @redis.with do |conn|
        # 使用 pipeline 批量提交订单
        conn.pipelined do |pipe|
          orders.each do |order|
            start_time = Time.now
            
            # 提交订单到对应交易对的队列
            pipe.lpush("pending_orders:#{@trading_pair}", Oj.dump(order))
            
            duration = ((Time.now - start_time) * 1000).round(2)
            @monitor.record_operation('order_submission', duration, @trading_pair)
          end
        end
      end
    end

    def print_metrics
      puts "\nPerformance Metrics:"
      puts "Order Submission Latency (ms):"
      ['p50', 'p95', 'p99'].each do |percentile|
        latency = @monitor.get_percentile('order_submission', percentile, @trading_pair)
        puts "  #{percentile}: #{latency}"
      end
    end
  end
end

if __FILE__ == $0
  # 测试配置
  REDIS_URL = ENV['REDIS_URL'] || 'redis://localhost:6379/0'
  REDIS_POOL_SIZE = (ENV['REDIS_POOL_SIZE'] || 5).to_i
  TRADING_PAIR = ENV['TRADING_PAIR'] || 'BTCUSDT'
  NUM_ORDERS = (ENV['NUM_ORDERS'] || 1000).to_i
  NUM_THREADS = (ENV['NUM_THREADS'] || 4).to_i
  BATCH_SIZE = (ENV['BATCH_SIZE'] || 100).to_i

  redis_pool = ConnectionPool.new(size: REDIS_POOL_SIZE) { Redis.new(url: REDIS_URL) }
  
  # 确保交易对已注册
  redis_pool.with { |conn| conn.sadd('trading_pairs', TRADING_PAIR) }
  
  test = ExchangeEngine::PerformanceTest.new(redis_pool, TRADING_PAIR)
  test.run_test(
    num_orders: NUM_ORDERS,
    num_threads: NUM_THREADS,
    batch_size: BATCH_SIZE
  )
end
