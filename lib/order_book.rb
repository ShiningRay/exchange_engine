require 'redis'
require 'connection_pool'
require 'oj'
require 'logger'
require 'securerandom'

module ExchangeEngine
  class OrderBook
    PRICE_PRECISION = 8  # 价格精度
    AMOUNT_PRECISION = 8 # 数量精度

    def initialize(symbol, redis_pool, logger = nil)
      @symbol = symbol
      @redis = redis_pool
      @logger = logger || Logger.new(STDOUT)
      @logger.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime}] #{severity} #{@symbol} - #{msg}\n"
      end
      @logger.info("OrderBook initialized for #{@symbol}")
    end

    # 添加限价订单
    def add_limit_order(order)
      @logger.info("Adding #{order[:side]} order: #{order.inspect}")
      @redis.with do |conn|
        order_key = order_key(order[:id])
        price_key = price_key(order[:side])
        
        conn.multi do |multi|
          # 保存订单详情
          multi.hset(order_key, 
            'id', order[:id],
            'price', order[:price].to_f,
            'amount', order[:amount].to_f,
            'side', order[:side],
            'status', 'open',
            'timestamp', Time.now.to_i
          )
          
          # 将订单添加到价格队列
          multi.zadd(price_key, order[:price], order[:id])
        end
      end
      
      @logger.debug("Order #{order[:id]} added to Redis")
      
      # 尝试撮合订单
      match_orders
    end

    # 取消订单
    def cancel_order(order_id)
      @logger.info("Attempting to cancel order: #{order_id}")
      @redis.with do |conn|
        order = conn.hgetall(order_key(order_id))
        if order.empty?
          @logger.warn("Order #{order_id} not found")
          return false
        end

        @logger.debug("Found order to cancel: #{order.inspect}")
        conn.multi do |multi|
          # 从价格队列中移除
          price_key = price_key(order['side'])
          multi.zrem(price_key, order_id)
          
          # 更新订单状态
          multi.hset(order_key(order_id), 'status', 'cancelled')
        end
      end
      @logger.info("Order #{order_id} cancelled successfully")
      true
    end

    def print_order_book
      @logger.info("Current Order Book Status for #{@symbol}")
      @redis.with do |conn|
        # 获取买单
        buy_orders = get_orders_by_side(conn, 'buy')
        @logger.info("Buy Orders:")
        print_orders(buy_orders)

        # 获取卖单
        sell_orders = get_orders_by_side(conn, 'sell')
        @logger.info("Sell Orders:")
        print_orders(sell_orders)
      end
    end

    def execute_trade(conn, bid_order, ask_order, amount, price)
      trade_id = "trade:#{Time.now.to_i}:#{SecureRandom.hex(4)}"
      @logger.info("Executing trade #{trade_id} - Amount: #{amount}, Price: #{price}")
      
      trade = {
        'id' => trade_id,
        'price' => price,
        'amount' => amount,
        'bid_order_id' => bid_order['id'],
        'ask_order_id' => ask_order['id'],
        'timestamp' => Time.now.to_i,
        'trading_pair' => @symbol
      }

      conn.multi do |multi|
        # 更新买方订单
        update_order_after_trade(multi, bid_order, amount)
        
        # 更新卖方订单
        update_order_after_trade(multi, ask_order, amount)

        # 记录成交到交易队列
        multi.lpush(trades_key, Oj.dump(trade))
        # 限制队列长度，保留最近的 1000 条记录
        multi.ltrim(trades_key, 0, 999)

        # 同时保存交易详情
        multi.hmset("#{@symbol}:#{trade_id}",
          'id', trade_id,
          'price', price,
          'amount', amount,
          'bid_order_id', bid_order['id'],
          'ask_order_id', ask_order['id'],
          'timestamp', trade['timestamp']
        )
      end
      
      @logger.info("Trade #{trade_id} executed successfully")
    end

    # 获取最近的成交记录
    def recent_trades(limit = 50)
      @logger.debug("Fetching recent trades, limit: #{limit}")
      trades = []
      
      @redis.with do |conn|
        raw_trades = conn.lrange(trades_key, 0, limit - 1)
        trades = raw_trades.map { |t| Oj.load(t) }
      end

      @logger.debug("Found #{trades.size} recent trades")
      trades
    end

    # 打印最近的成交记录
    def print_recent_trades(limit = 50)
      @logger.info("Recent Trades for #{@symbol}")
      trades = recent_trades(limit)
      
      if trades.empty?
        @logger.info("  No trades found")
        return
      end

      trades.each do |trade|
        @logger.info("  Trade ID: #{trade['id']}")
        @logger.info("    Price: #{trade['price']}")
        @logger.info("    Amount: #{trade['amount']}")
        @logger.info("    Time: #{Time.at(trade['timestamp'])}")
        @logger.info("    Bid Order: #{trade['bid_order_id']}")
        @logger.info("    Ask Order: #{trade['ask_order_id']}")
        @logger.info("    ---------------")
      end
    end

    private

    def match_orders
      @logger.debug("Starting order matching process")
      @redis.with do |conn|
        loop do
          # 获取最高买价和最低卖价
          highest_bid = get_best_bid(conn)
          lowest_ask = get_best_ask(conn)

          @logger.debug("Best bid: #{highest_bid}, Best ask: #{lowest_ask}")

          # 如果没有可以撮合的订单，退出循环
          if highest_bid.nil? || lowest_ask.nil?
            @logger.debug("No matching possible - missing orders")
            break
          end

          if highest_bid < lowest_ask
            @logger.debug("No matching possible - bid #{highest_bid} < ask #{lowest_ask}")
            break
          end

          # 执行撮合
          execute_match(conn, highest_bid, lowest_ask)
        end
      end
      @logger.debug("Order matching process completed")
    end

    def get_best_bid(conn)
      bid_orders = conn.zrevrange(price_key('buy'), 0, 0, with_scores: true)
      result = bid_orders.empty? ? nil : bid_orders[0][1]
      @logger.debug("Best bid price: #{result}")
      result
    end

    def get_best_ask(conn)
      ask_orders = conn.zrange(price_key('sell'), 0, 0, with_scores: true)
      result = ask_orders.empty? ? nil : ask_orders[0][1]
      @logger.debug("Best ask price: #{result}")
      result
    end

    def execute_match(conn, bid_price, ask_price)
      @logger.info("Executing match - Bid: #{bid_price}, Ask: #{ask_price}")
      
      # 获取这个价格级别的订单
      bid_orders = conn.zrangebyscore(price_key('buy'), bid_price, bid_price)
      ask_orders = conn.zrangebyscore(price_key('sell'), ask_price, ask_price)

      if bid_orders.empty? || ask_orders.empty?
        @logger.warn("No orders found at price levels - Bid orders: #{bid_orders.size}, Ask orders: #{ask_orders.size}")
        return
      end

      bid_order = conn.hgetall(order_key(bid_orders[0]))
      ask_order = conn.hgetall(order_key(ask_orders[0]))

      if bid_order.empty? || ask_order.empty?
        @logger.warn("Order details not found - Bid: #{bid_orders[0]}, Ask: #{ask_orders[0]}")
        return
      end

      # 计算成交量
      match_amount = [bid_order['amount'].to_f, ask_order['amount'].to_f].min
      match_price = ask_price  # 通常采用卖方价格

      @logger.info("Match found - Amount: #{match_amount}, Price: #{match_price}")
      @logger.debug("Bid order: #{bid_order.inspect}")
      @logger.debug("Ask order: #{ask_order.inspect}")

      # 执行成交
      execute_trade(conn, bid_order, ask_order, match_amount, match_price)
    end

    def update_order_after_trade(multi, order, traded_amount)
      remaining = order['amount'].to_f - traded_amount
      order_id = order['id']
      side = order['side']

      @logger.debug("Updating order #{order_id} - Traded: #{traded_amount}, Remaining: #{remaining}")

      if remaining <= 0
        # 订单完全成交
        @logger.info("Order #{order_id} fully filled")
        multi.del(order_key(order_id))
        multi.zrem(price_key(side), order_id)
      else
        # 部分成交
        @logger.info("Order #{order_id} partially filled - Remaining: #{remaining}")
        multi.hset(order_key(order_id), 'amount', remaining)
      end
    end

    def get_orders_by_side(conn, side)
      price_key = price_key(side)
      # 获取所有价格级别
      prices = if side == 'buy'
                conn.zrevrange(price_key, 0, -1, with_scores: true)
              else
                conn.zrange(price_key, 0, -1, with_scores: true)
              end

      orders = []
      prices.each do |order_id, price|
        order = conn.hgetall(order_key(order_id))
        orders << order unless order.empty?
      end
      orders
    end

    def print_orders(orders)
      if orders.empty?
        @logger.info("  No orders")
        return
      end

      # 按价格分组
      orders_by_price = orders.group_by { |order| order['price'] }
      
      orders_by_price.each do |price, price_orders|
        total_amount = price_orders.sum { |order| order['amount'].to_f }
        @logger.info("  Price: #{price}")
        @logger.info("    Total Amount: #{total_amount}")
        @logger.info("    Orders:")
        price_orders.each do |order|
          @logger.info("      ID: #{order['id']}, Amount: #{order['amount']}, Time: #{Time.at(order['timestamp'].to_i)}")
        end
      end
    end

    def order_key(order_id)
      "order:#{@symbol}:#{order_id}"
    end

    def price_key(side)
      "#{@symbol}:#{side}_orders"
    end

    def trades_key
      "#{@symbol}:trades"
    end
  end
end
