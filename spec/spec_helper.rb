require 'rspec'
require 'mock_redis'
require 'connection_pool'

# 这里我们使用 mock_redis 来模拟 Redis 服务器
RSpec.configure do |config|
  config.before(:each) do
    @redis = MockRedis.new
    @redis_pool = ConnectionPool.new(size: 5) { @redis }
  end
end
