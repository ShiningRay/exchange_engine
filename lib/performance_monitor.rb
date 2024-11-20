require 'redis'
require 'connection_pool'

module ExchangeEngine
  class PerformanceMonitor
    def initialize(redis_pool)
      @redis = redis_pool
    end

    def record_operation(operation, duration, trading_pair)
      @redis.with do |conn|
        key = "metrics:#{trading_pair}:#{operation}"
        timestamp = Time.now.to_i

        # 记录操作耗时
        conn.zadd(key, timestamp, duration)
        
        # 维护最近1小时的数据
        one_hour_ago = timestamp - 3600
        conn.zremrangebyscore(key, '-inf', one_hour_ago)
        
        # 更新操作计数
        count_key = "count:#{trading_pair}:#{operation}"
        conn.incr(count_key)
      end
    end

    def get_metrics
      metrics = {}
      
      @redis.with do |conn|
        # 获取所有交易对
        trading_pairs = conn.smembers('trading_pairs')
        
        trading_pairs.each do |pair|
          # 获取该交易对的所有指标key
          operation_keys = conn.keys("metrics:#{pair}:*")
          count_keys = conn.keys("count:#{pair}:*")
          
          operation_keys.each do |key|
            operation = key.split(':').last
            values = conn.zrange(key, 0, -1, with_scores: true).map(&:first).map(&:to_f)
            next if values.empty?
            
            metrics["#{pair}:#{operation}"] = calculate_metrics(values, operation, pair)
          end

          # 获取队列长度
          queue_length = conn.llen("pending_orders:#{pair}")
          metrics["#{pair}:queue_length"] = queue_length

          # 获取订单簿状态
          buy_orders = conn.zcard("#{pair}:buy_orders")
          sell_orders = conn.zcard("#{pair}:sell_orders")
          metrics["#{pair}:buy_orders"] = buy_orders
          metrics["#{pair}:sell_orders"] = sell_orders
        end
      end
      
      metrics
    end

    def get_percentile(operation, percentile_str, trading_pair)
      @redis.with do |conn|
        key = "metrics:#{trading_pair}:#{operation}"
        values = conn.zrange(key, 0, -1, with_scores: true).map(&:first).map(&:to_f)
        return 0 if values.empty?

        p = case percentile_str
        when 'p50' then 50
        when 'p95' then 95
        when 'p99' then 99
        else 50
        end

        percentile(values, p).round(3)
      end
    end

    private

    def calculate_metrics(values, operation, pair)
      {
        avg: (values.sum / values.size).round(3),
        max: values.max.round(3),
        min: values.min.round(3),
        p95: percentile(values, 95).round(3),
        p99: percentile(values, 99).round(3),
        count: @redis.with { |conn| conn.get("count:#{pair}:#{operation}").to_i }
      }
    end

    def percentile(values, p)
      return 0 if values.empty?
      values = values.sort
      k = (values.length - 1) * p / 100.0
      f = k.floor
      c = k.ceil
      if f == c
        values[f]
      else
        (values[f] * (c - k) + values[c] * (k - f))
      end
    end
  end
end
