require_relative 'spec_helper'
require_relative '../lib/order_book'

RSpec.describe ExchangeEngine::OrderBook do
  let(:symbol) { 'BTCUSDT' }
  let(:order_book) { described_class.new(symbol, @redis_pool) }

  describe '#add_limit_order' do
    context '添加买单' do
      let(:buy_order) do
        {
          id: 'order1',
          side: 'buy',
          price: 30000.0,
          amount: 1.0,
          timestamp: Time.now.to_i
        }
      end

      it '成功添加买单' do
        expect { order_book.add_limit_order(buy_order) }.not_to raise_error
        
        order_key = "order:#{symbol}:#{buy_order[:id]}"
        stored_order = @redis.hgetall(order_key)
        
        expect(stored_order['id']).to eq(buy_order[:id])
        expect(stored_order['price'].to_f).to eq(buy_order[:price])
        expect(stored_order['amount'].to_f).to eq(buy_order[:amount])
        expect(stored_order['side']).to eq(buy_order[:side])
        expect(stored_order['status']).to eq('open')
      end
    end

    context '订单撮合' do
      let(:buy_order) do
        {
          id: 'buy1',
          side: 'buy',
          price: 30000.0,
          amount: 1.5,
          timestamp: Time.now.to_i
        }
      end

      let(:sell_order) do
        {
          id: 'sell1',
          side: 'sell',
          price: 30000.0,
          amount: 1.0,
          timestamp: Time.now.to_i
        }
      end

      it '相同价格的订单可以撮合' do
        order_book.add_limit_order(buy_order)
        order_book.add_limit_order(sell_order)

        # 检查卖单是否被完全成交
        sell_order_key = "order:#{symbol}:#{sell_order[:id]}"
        expect(@redis.exists?(sell_order_key)).to be false

        # 检查买单是否部分成交
        buy_order_key = "order:#{symbol}:#{buy_order[:id]}"
        remaining_buy_order = @redis.hgetall(buy_order_key)
        expect(remaining_buy_order['amount'].to_f).to eq(0.5)
      end
    end
  end

  describe '#cancel_order' do
    let(:order) do
      {
        id: 'order1',
        side: 'buy',
        price: 30000.0,
        amount: 1.0,
        timestamp: Time.now.to_i
      }
    end

    it '成功取消订单' do
      order_book.add_limit_order(order)
      expect(order_book.cancel_order(order[:id])).to be true

      order_key = "order:#{symbol}:#{order[:id]}"
      stored_order = @redis.hgetall(order_key)
      expect(stored_order['status']).to eq('cancelled')
    end

    it '取消不存在的订单返回false' do
      expect(order_book.cancel_order('non_existent_order')).to be false
    end
  end

  context '价格优先级测试' do
    let(:buy_order_1) do
      {
        id: 'buy1',
        side: 'buy',
        price: 30000.0,
        amount: 1.0,
        timestamp: Time.now.to_i
      }
    end

    let(:buy_order_2) do
      {
        id: 'buy2',
        side: 'buy',
        price: 30100.0,
        amount: 1.0,
        timestamp: Time.now.to_i
      }
    end

    let(:sell_order) do
      {
        id: 'sell1',
        side: 'sell',
        price: 30000.0,
        amount: 1.0,
        timestamp: Time.now.to_i + 1
      }
    end

    it '高价买单优先撮合' do
      order_book.add_limit_order(buy_order_1)
      order_book.add_limit_order(buy_order_2)
      order_book.add_limit_order(sell_order)

      # 检查高价买单是否被撮合
      expect(@redis.exists?("order:#{symbol}:#{buy_order_2[:id]}")).to be false
      # 检查低价买单是否仍然存在
      expect(@redis.exists?("order:#{symbol}:#{buy_order_1[:id]}")).to be true
    end
  end
end
