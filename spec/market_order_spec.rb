require 'spec_helper'
require 'connection_pool'
require 'redis'
require 'oj'
require_relative '../lib/order_processor'

RSpec.describe ExchangeEngine::OrderBook do
  let(:redis_pool) { ConnectionPool.new(size: 5) { Redis.new(url: ENV['REDIS_URL'] || 'redis://localhost:6379/0') } }
  let(:order_book) { described_class.new('BTCUSDT', redis_pool) }
  let(:processor) { ExchangeEngine::OrderProcessor.new(redis_pool, 'BTCUSDT') }

  before(:each) do
    # 清理 Redis 数据
    redis_pool.with { |conn| conn.flushdb }
  end

  describe 'market orders' do
    context 'when matching with limit orders' do
      before do
        # 添加一些限价单作为对手单
        limit_orders = [
          {
            'id' => 'sell1',
            'type' => 'limit',
            'side' => 'sell',
            'price' => '50000.00',
            'amount' => '1.0',
            'trading_pair' => 'BTCUSDT'
          },
          {
            'id' => 'sell2',
            'type' => 'limit',
            'side' => 'sell',
            'price' => '50100.00',
            'amount' => '2.0',
            'trading_pair' => 'BTCUSDT'
          }
        ]

        limit_orders.each do |order|
          processor.instance_variable_get(:@order_book).add_limit_order(order)
        end
      end

      it 'executes market buy order fully when enough liquidity' do
        market_order = {
          'id' => 'market_buy1',
          'type' => 'market',
          'side' => 'buy',
          'amount' => '0.5',
          'trading_pair' => 'BTCUSDT'
        }

        expect(order_book.add_market_order(market_order)).to be true

        # 验证订单状态
        redis_pool.with do |conn|
          order = conn.hgetall("order:BTCUSDT:market_buy1")
          expect(order['status']).not_to eq('failed')
          expect(order['status']).not_to eq('partially_filled')
        end
      end

      it 'handles partial fills for market orders' do
        market_order = {
          'id' => 'market_buy2',
          'type' => 'market',
          'side' => 'buy',
          'amount' => '5.0',  # 大于可用流动性
          'trading_pair' => 'BTCUSDT'
        }

        expect(order_book.add_market_order(market_order)).to be true

        # 验证订单状态
        redis_pool.with do |conn|
          order = conn.hgetall("order:BTCUSDT:market_buy2")
          expect(order['status']).to eq('partially_filled')
          expect(BigDecimal(order['remaining_amount'])).to be > 0
        end
      end

      it 'handles market order with no matching orders' do
        # 清空所有订单
        redis_pool.with { |conn| conn.flushdb }

        market_order = {
          'id' => 'market_buy3',
          'type' => 'market',
          'side' => 'buy',
          'amount' => '1.0',
          'trading_pair' => 'BTCUSDT'
        }

        expect(order_book.add_market_order(market_order)).to be false

        # 验证订单状态
        redis_pool.with do |conn|
          order = conn.hgetall("order:BTCUSDT:market_buy3")
          expect(order['status']).to eq('failed')
          expect(order['error']).to eq('No matching orders available')
        end
      end
    end

    context 'when testing market sell orders' do
      before do
        # 添加一些买单作为对手单
        limit_orders = [
          {
            'id' => 'buy1',
            'type' => 'limit',
            'side' => 'buy',
            'price' => '49900.00',
            'amount' => '1.0',
            'trading_pair' => 'BTCUSDT'
          },
          {
            'id' => 'buy2',
            'type' => 'limit',
            'side' => 'buy',
            'price' => '49800.00',
            'amount' => '2.0',
            'trading_pair' => 'BTCUSDT'
          }
        ]

        limit_orders.each do |order|
          processor.instance_variable_get(:@order_book).add_limit_order(order)
        end
      end

      it 'executes market sell order at best available price' do
        market_order = {
          'id' => 'market_sell1',
          'type' => 'market',
          'side' => 'sell',
          'amount' => '0.5',
          'trading_pair' => 'BTCUSDT'
        }

        expect(order_book.add_market_order(market_order)).to be true

        # 验证成交记录
        trades = order_book.recent_trades(1)
        expect(trades.length).to eq(1)
        expect(BigDecimal(trades[0]['price'])).to eq(BigDecimal('49900.00'))
      end

      it 'handles market sell order price slippage' do
        market_order = {
          'id' => 'market_sell2',
          'type' => 'market',
          'side' => 'sell',
          'amount' => '1.5',
          'trading_pair' => 'BTCUSDT'
        }

        expect(order_book.add_market_order(market_order)).to be true

        # 验证成交记录
        trades = order_book.recent_trades(2)
        expect(trades.length).to eq(2)
        # binding.pry
        # 验证价格滑点
        prices = trades.map { |t| BigDecimal(t['price']) }.sort.reverse
        expect(prices[0]).to eq(BigDecimal('49900.00'))
        expect(prices[1]).to eq(BigDecimal('49800.00'))
      end
    end
  end
end
