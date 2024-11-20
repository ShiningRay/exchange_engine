require 'redis'
require 'connection_pool'
require 'oj'
require 'logger'
require 'bigdecimal'
require_relative 'order_book'

module ExchangeEngine
  class OrderProcessor
    attr_reader :trading_pair

    def initialize(redis_pool, trading_pair, logger = nil)
      @redis = redis_pool
      @trading_pair = trading_pair
      @logger = logger || Logger.new(STDOUT)
      @logger.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime}] #{severity} OrderProcessor(#{trading_pair}) - #{msg}\n"
      end
      @running = false
      @order_book = OrderBook.new(trading_pair, @redis, @logger)
    end

    def start
      return if @running
      @running = true
      @logger.info("Order processor starting for #{@trading_pair}...")
      
      # 开始处理循环
      process_loop
    end

    def stop
      return unless @running
      @running = false
      @logger.info("Order processor stopping for #{@trading_pair}...")
    end

    private

    def queue_name
      "pending_orders:#{@trading_pair}"
    end

    def failed_queue_name
      "failed_orders:#{@trading_pair}"
    end

    def process_loop
      while @running
        begin
          process_next_order
        rescue => e
          @logger.error("Error processing order: #{e.message}")
          @logger.error(e.backtrace.join("\n"))
        end
        sleep(0.001) # 避免CPU空转
      end
    end

    def process_next_order
      @redis.with do |conn|
        # 从订单队列中取出订单（阻塞1秒）
        _, raw_order = conn.brpop(queue_name, 1)
        return unless raw_order

        begin
          order = Oj.load(raw_order)
          @logger.info("Processing order: #{order.inspect}")

          # 验证订单格式
          unless order.is_a?(Hash) && order['trading_pair'] && order['type'] && order['side']
            error_msg = "Invalid order format: #{order.inspect}"
            @logger.error(error_msg)
            conn.lpush(failed_queue_name, Oj.dump({
              'order' => order,
              'error' => error_msg,
              'timestamp' => Time.now.to_i
            }))
            return
          end

          # 验证交易对
          unless order['trading_pair'] == @trading_pair
            error_msg = "Trading pair mismatch: expected #{@trading_pair}, got #{order['trading_pair']}"
            @logger.error(error_msg)
            # 将订单重新放入正确的队列
            correct_queue = "pending_orders:#{order['trading_pair']}"
            conn.lpush(correct_queue, raw_order)
            @logger.info("Redirected order to correct queue: #{correct_queue}")
            return
          end

          # 转换订单格式，使用 BigDecimal
          processed_order = {
            'id' => order['id'],
            'side' => order['side'],
            'price' => BigDecimal(order['price'].to_s).to_s('F'),
            'amount' => BigDecimal(order['amount'].to_s).to_s('F'),
            'timestamp' => order['timestamp'] || Time.now.to_i,
            'type' => order['type'],
            'trading_pair' => order['trading_pair']
          }

          # 验证价格和数量
          price = BigDecimal(processed_order['price'])
          amount = BigDecimal(processed_order['amount'])

          if price.negative? || price.zero?
            error_msg = "Invalid price: #{price}"
            @logger.error(error_msg)
            conn.lpush(failed_queue_name, Oj.dump({
              'order' => order,
              'error' => error_msg,
              'timestamp' => Time.now.to_i
            }))
            return
          end

          if amount.negative? || amount.zero?
            error_msg = "Invalid amount: #{amount}"
            @logger.error(error_msg)
            conn.lpush(failed_queue_name, Oj.dump({
              'order' => order,
              'error' => error_msg,
              'timestamp' => Time.now.to_i
            }))
            return
          end

          # 处理订单
          case order['type']
          when 'limit'
            @order_book.add_limit_order(processed_order)
          when 'cancel'
            @order_book.cancel_order(processed_order['id'])
          else
            error_msg = "Unknown order type: #{order['type']}"
            @logger.error(error_msg)
            conn.lpush(failed_queue_name, Oj.dump({
              'order' => order,
              'error' => error_msg,
              'timestamp' => Time.now.to_i
            }))
          end

        rescue => e
          @logger.error("Failed to process order: #{e.message}")
          @logger.error(e.backtrace.join("\n"))
          
          # 将失败的订单放入失败队列
          conn.lpush(failed_queue_name, Oj.dump({
            'order' => order,
            'error' => e.message,
            'timestamp' => Time.now.to_i
          }))
        end
      end
    end
  end

  class OrderProcessorManager
    def initialize(redis_pool, logger = nil)
      @redis = redis_pool
      @logger = logger || Logger.new(STDOUT)
      @logger.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime}] #{severity} OrderProcessorManager - #{msg}\n"
      end
      @processors = {}
      @running = false
    end

    def start
      return if @running
      @running = true

      @redis.with do |conn|
        trading_pairs = conn.smembers('trading_pairs')
        @logger.info("Starting processors for trading pairs: #{trading_pairs.join(', ')}")
        
        trading_pairs.each do |pair|
          @logger.info("Starting processor for #{pair}")
          processor = OrderProcessor.new(@redis, pair, @logger)
          Thread.new { 
            begin
              processor.start
            rescue => e
              @logger.error("Processor for #{pair} failed: #{e.message}")
              @logger.error(e.backtrace.join("\n"))
            end
          }
          @processors[pair] = processor
        end
      end
    end

    def stop
      return unless @running
      @running = false
      
      @processors.each do |pair, processor|
        @logger.info("Stopping processor for #{pair}")
        processor.stop
      end
      @processors.clear
    end

    def processor_for(trading_pair)
      @processors[trading_pair]
    end
  end
end
