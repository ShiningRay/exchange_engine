require 'redis'
require 'connection_pool'
require 'oj'
require 'logger'
require 'securerandom'
require 'bigdecimal'

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
      @logger.info("Adding #{order['side']} order: #{order.inspect}")
      @redis.with do |conn|
        order_key = order_key(order['id'])
        price_key = price_key(order['side'])
        
        # 将 BigDecimal 转换为字符串
        price = BigDecimal(order['price'].to_s).to_s('F')
        amount = BigDecimal(order['amount'].to_s).to_s('F')
        
        conn.multi do |multi|
          # 保存订单详情
          multi.hset(order_key, 
            'id', order['id'],
            'price', price,
            'amount', amount,
            'side', order['side'],
            'status', 'open',
            'timestamp', Time.now.to_i
          )
          
          # 将订单添加到价格队列，使用字符串形式的价格
          multi.zadd(price_key, price, order['id'])
        end
      end
      
      @logger.debug("Order #{order['id']} added to Redis")
      
      # 尝试撮合订单
      match_orders
    end

    # 添加市价单
    def add_market_order(order)
      @logger.info("Adding market #{order['side']} order: #{order.inspect}")
      amount = BigDecimal(order['amount'].to_s)
      
      @redis.with do |conn|
        order_key = order_key(order['id'])
        
        # 获取对手方最优价格订单
        counter_side = order['side'] == 'buy' ? 'sell' : 'buy'
        best_price = order['side'] == 'buy' ? get_best_ask(conn) : get_best_bid(conn)
        
        if best_price.nil?
          @logger.warn("No matching orders available for market order #{order['id']}")
          conn.hset(order_key,
            'id', order['id'],
            'amount', amount.to_s('F'),
            'side', order['side'],
            'status', 'failed',
            'error', 'No matching orders available',
            'timestamp', Time.now.to_i,
            'type', 'market'
          )
          return false
        end

        # 保存订单信息
        conn.hset(order_key,
          'id', order['id'],
          'amount', amount.to_s('F'),
          'side', order['side'],
          'status', 'open',
          'timestamp', Time.now.to_i,
          'type', 'market'
        )
        
        # 立即尝试撮合
        execute_market_order(conn, order['id'], amount, order['side'])
      end
      true
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
        'price' => BigDecimal(price.to_s).to_s('F'),
        'amount' => BigDecimal(amount.to_s).to_s('F'),
        'bid_order_id' => bid_order['id'],
        'ask_order_id' => ask_order['id'],
        'timestamp' => Time.now.to_i,
        'trading_pair' => @symbol
      }

      conn.multi do |multi|
        # 记录成交
        multi.rpush(trades_key, Oj.dump(trade))
        multi.ltrim(trades_key, -1000, -1)  # 保留最近1000条成交记录

        # 更新订单状态
        update_order_after_trade(multi, bid_order, amount)
        update_order_after_trade(multi, ask_order, amount)
      end

      @logger.info("Trade #{trade_id} executed successfully")
      trade
    end

    # 获取最近的成交记录
    def recent_trades(limit = 50)
      @redis.with do |conn|
        trades = conn.lrange(trades_key, -limit, -1)
        trades.map { |t| Oj.load(t) }
      end
    end

    # 打印最近的成交记录
    def print_recent_trades(limit = 50)
      trades = recent_trades(limit)
      @logger.info("Recent Trades for #{@symbol}:")
      trades.each do |trade|
        @logger.info("#{trade['timestamp']} - Price: #{trade['price']}, Amount: #{trade['amount']}")
      end
    end

    private

    def match_orders
      @redis.with do |conn|
        loop do
          # 获取最优买卖价格
          bid_price = get_best_bid(conn)
          ask_price = get_best_ask(conn)

          break if bid_price.nil? || ask_price.nil?
          bid_price = BigDecimal(bid_price.to_s)
          ask_price = BigDecimal(ask_price.to_s)

          # 如果买价小于卖价，无法撮合
          break if bid_price < ask_price

          # 执行撮合
          executed = execute_match(conn, bid_price.to_s('F'), ask_price.to_s('F'))
          break unless executed
        end
      end
    end

    def get_best_bid(conn)
      # 获取买方最高价格
      result = conn.zrevrange(price_key('buy'), 0, 0, with_scores: true)
      result.empty? ? nil : result[0][1].to_s
    end

    def get_best_ask(conn)
      # 获取卖方最低价格
      result = conn.zrange(price_key('sell'), 0, 0, with_scores: true)
      result.empty? ? nil : result[0][1].to_s
    end

    def execute_match(conn, bid_price, ask_price)
      # 获取对应价格的订单
      bid_orders = conn.zrangebyscore(price_key('buy'), bid_price, bid_price)
      ask_orders = conn.zrangebyscore(price_key('sell'), ask_price, ask_price)

      return false if bid_orders.empty? || ask_orders.empty?

      # 获取订单详情
      bid_order = conn.hgetall(order_key(bid_orders[0]))
      ask_order = conn.hgetall(order_key(ask_orders[0]))

      # 计算成交数量
      bid_amount = BigDecimal(bid_order['amount'])
      ask_amount = BigDecimal(ask_order['amount'])
      trade_amount = [bid_amount, ask_amount].min

      # 执行成交
      execute_trade(conn, bid_order, ask_order, trade_amount.to_s('F'), ask_price)

      true
    end

    def execute_market_order(conn, order_id, remaining_amount, side)
      while remaining_amount > 0
        # 获取对手方最优价格订单
        best_price = side == 'buy' ? get_best_ask(conn) : get_best_bid(conn)
        break if best_price.nil?

        # 获取该价格的所有订单
        counter_side = side == 'buy' ? 'sell' : 'buy'
        orders = conn.zrangebyscore(price_key(counter_side), best_price, best_price)
        break if orders.empty?

        # 遍历订单进行撮合
        orders.each do |counter_order_id|
          counter_order = conn.hgetall(order_key(counter_order_id))
          next if counter_order.empty? || counter_order['status'] != 'open'

          counter_amount = BigDecimal(counter_order['amount'])
          trade_amount = [remaining_amount, counter_amount].min
          
          # 执行交易
          execute_trade(conn, 
            side == 'buy' ? {'id' => order_id, 'side' => 'buy'} : counter_order,
            side == 'buy' ? counter_order : {'id' => order_id, 'side' => 'sell'},
            trade_amount.to_s('F'),
            best_price
          )

          remaining_amount -= trade_amount
          break if remaining_amount <= 0
        end
      end

      # 如果还有剩余数量，更新订单状态为部分成交
      if remaining_amount > 0
        conn.hset(order_key(order_id), 
          'status', 'partially_filled',
          'remaining_amount', remaining_amount.to_s('F')
        )
      end
    end

    def update_order_after_trade(multi, order, traded_amount)
      order_key = order_key(order['id'])
      price_key = price_key(order['side'])
      
      remaining_amount = BigDecimal(order['amount']) - BigDecimal(traded_amount.to_s)
      
      if remaining_amount.zero?
        # 订单完全成交，从价格队列中移除
        multi.zrem(price_key, order['id'])
        multi.hset(order_key, 'status', 'filled')
      else
        # 订单部分成交，更新剩余数量
        multi.hset(order_key, 'amount', remaining_amount.to_s('F'))
      end
    end

    def get_orders_by_side(conn, side)
      price_key = price_key(side)
      order_ids = conn.zrange(price_key, 0, -1, with_scores: true)
      
      orders = order_ids.map do |id, price|
        order = conn.hgetall(order_key(id))
        next if order.empty?
        order.merge('price' => price)
      end.compact
      
      side == 'buy' ? orders.reverse : orders
    end

    def print_orders(orders)
      orders.each do |order|
        @logger.info("#{order['id']} - Price: #{order['price']}, Amount: #{order['amount']}")
      end
    end

    def order_key(order_id)
      "order:#{@symbol}:#{order_id}"
    end

    def price_key(side)
      "#{@symbol}:#{side}_orders"
    end

    def trades_key
      "trades:#{@symbol}"
    end
  end
end
