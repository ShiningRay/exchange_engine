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
        'amount' => amount,
        'bid_order_id' => bid_order['id'],
        'ask_order_id' => ask_order['id'],
        'timestamp' => Time.now.to_i
      }

      conn.multi do |multi|
        # 更新买单状态
        update_order_after_trade(multi, bid_order, amount) if bid_order['type'] == 'limit'
        
        # 更新卖单状态
        update_order_after_trade(multi, ask_order, amount) if ask_order['type'] == 'limit'
        
        # 记录成交
        multi.lpush(trades_key, Oj.dump(trade))
      end
      
      @logger.info("Trade executed: #{trade.inspect}")
      trade
    end

    # 获取最近的成交记录
    def recent_trades(limit = 50)
      @redis.with do |conn|
        trades = conn.lrange(trades_key, -limit, -1)
        trades.map { |t| Oj.load(t) }
      end
    end

    def print_recent_trades(limit = 50)
      trades = recent_trades(limit)
      @logger.info("Recent Trades for #{@symbol}:")
      trades.each do |trade|
        @logger.info("#{trade['timestamp']} - Price: #{trade['price']}, Amount: #{trade['amount']}")
      end
    end

    # 获取市场深度数据
    def get_market_depth(levels = 20)
      @logger.info("Calculating market depth for #{@symbol}, levels: #{levels}")
      
      @redis.with do |conn|
        # 获取买单深度
        bids = calculate_depth(conn, 'buy', levels)
        
        # 获取卖单深度
        asks = calculate_depth(conn, 'sell', levels)
        
        {
          'bids' => bids,
          'asks' => asks,
          'timestamp' => Time.now.to_i
        }
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
      original_amount = remaining_amount
      trades = []
      total_executed = BigDecimal('0')
      
      while remaining_amount > 0
        # 获取对手方最优价格订单
        best_price = side == 'buy' ? get_best_ask(conn) : get_best_bid(conn)
        break if best_price.nil?

        # 获取该价格的所有订单
        counter_side = side == 'buy' ? 'sell' : 'buy'
        orders = conn.zrangebyscore(price_key(counter_side), best_price, best_price)
        break if orders.empty?

        # 遍历订单进行撮合
        price_level_amount = BigDecimal('0')
        orders.each do |counter_order_id|
          counter_order = conn.hgetall(order_key(counter_order_id))
          next if counter_order.empty? || counter_order['status'] != 'open'

          counter_amount = BigDecimal(counter_order['amount'])
          trade_amount = [remaining_amount, counter_amount].min
          
          # 执行交易
          # 记录交易
          trade = {
            'id' => "trade:#{Time.now.to_i}:#{SecureRandom.hex(4)}",
            'price' => best_price,
            'amount' => trade_amount.to_s('F'),
            'bid_order_id' => side == 'buy' ? order_id : counter_order['id'],
            'ask_order_id' => side == 'buy' ? counter_order['id'] : order_id,
            'timestamp' => Time.now.to_i
          }
          conn.lpush(trades_key, Oj.dump(trade))

          # 更新对手方订单
          update_order_after_trade(conn, counter_order, trade_amount)
          
          if trade
            trades << trade
            total_executed += trade_amount
            remaining_amount -= trade_amount
            price_level_amount += trade_amount

            # 更新订单状态
            if remaining_amount > 0
              conn.hset(order_key(order_id), 
                'status', 'partially_filled',
                'remaining_amount', remaining_amount.to_s('F')
              )
            else
              conn.hset(order_key(order_id), 
                'status', 'filled',
                'remaining_amount', '0'
              )
              break
            end
          end
        end

        # 如果当前价格档位已经用完，继续下一个价格档位
        break if remaining_amount <= 0
      end

      # 更新市价单最终状态
      if total_executed == 0
        conn.hset(order_key(order_id), 
          'status', 'failed',
          'error', 'No matching orders available'
        )
        return false
      elsif total_executed < original_amount
        conn.hset(order_key(order_id), 
          'status', 'partially_filled',
          'remaining_amount', remaining_amount.to_s('F')
        )
      else
        conn.hset(order_key(order_id), 
          'status', 'filled',
          'remaining_amount', '0'
        )
      end

      true
    end

    def update_order_after_trade(multi, order, traded_amount)
      order_key = order_key(order['id'])
      price_key = price_key(order['side'])
      
      # 获取订单当前数量
      current_amount = BigDecimal(order['amount'])
      traded = BigDecimal(traded_amount.to_s)
      remaining_amount = current_amount - traded

      if remaining_amount <= 0
        # 订单完全成交，从价格队列中移除
        multi.zrem(price_key, order['id'])
        multi.hset(order_key, 'status', 'filled', 'remaining_amount', '0')
      else
        # 订单部分成交，更新剩余数量
        multi.hset(order_key, 
          'amount', remaining_amount.to_s('F'),
          'status', 'partially_filled',
          'remaining_amount', remaining_amount.to_s('F')
        )
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

    def calculate_depth(conn, side, levels)
      price_key = price_key(side)
      depth = []
      total_volume = BigDecimal('0')
      
      # 获取排序后的价格和订单ID
      orders = if side == 'buy'
        # 买单按价格降序排列
        conn.zrevrange(price_key, 0, levels - 1, with_scores: true)
      else
        # 卖单按价格升序排列
        conn.zrange(price_key, 0, levels - 1, with_scores: true)
      end

      # 按价格分组计算数量
      current_price = nil
      current_amount = BigDecimal('0')
      
      orders.each do |order_id, price|
        order = conn.hgetall(order_key(order_id))
        next if order.empty? || order['status'] != 'open'
        
        price = BigDecimal(price.to_s)
        amount = BigDecimal(order['amount'].to_s)
        
        if current_price != price && !current_price.nil?
          # 添加当前价格层级的深度数据
          total_volume += current_amount
          depth << {
            'price' => current_price.to_s('F'),
            'amount' => current_amount.to_s('F'),
            'total' => total_volume.to_s('F')
          }
          current_amount = BigDecimal('0')
        end
        
        current_price = price
        current_amount += amount
      end
      
      # 添加最后一个价格层级
      if current_price && current_amount > 0
        total_volume += current_amount
        depth << {
          'price' => current_price.to_s('F'),
          'amount' => current_amount.to_s('F'),
          'total' => total_volume.to_s('F')
        }
      end
      
      depth
    end

    # 获取指定价格的订单总量
    def get_volume_at_price(conn, side, price)
      price_str = BigDecimal(price.to_s).to_s('F')
      orders = conn.zrangebyscore(price_key(side), price_str, price_str)
      
      total_volume = BigDecimal('0')
      orders.each do |order_id|
        order = conn.hgetall(order_key(order_id))
        next if order.empty? || order['status'] != 'open'
        total_volume += BigDecimal(order['amount'].to_s)
      end
      
      total_volume
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
