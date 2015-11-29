# encoding: utf-8
require "logstash/namespace"
require "logstash/inputs/base"
require "logstash/inputs/threadable"

# This input will read events from a Redis instance; it supports both Redis channels and lists.
# The list command (BLPOP) used by Logstash is supported in Redis v1.3.1+, and
# the channel commands used by Logstash are found in Redis v1.3.8+.
# While you may be able to make these Redis versions work, the best performance
# and stability will be found in more recent stable versions.  Versions 2.6.0+
# are recommended.
#
# For more information about Redis, see <http://redis.io/>
#
# `batch_count` note: If you use the `batch_count` setting, you *must* use a Redis version 2.6.0 or
# newer. Anything older does not support the operations used by batching.
#
module LogStash module Inputs class RedisCluster < LogStash::Inputs::Threadable
# class LogStash::Inputs::Redis < LogStash::Inputs::Threadable

  config_name "redis_cluster"

  default :codec, "json"

  # The `name` configuration is used for logging in case there are multiple instances.
  # This feature has no real function and will be removed in future versions.
  config :name, :validate => :string, :default => "default", :deprecated => true

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"
  
  # The hostname of your Redis server.
  config :driver, :validate => :string, :default => "jedis"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Initial connection timeout in seconds.
  config :timeout, :validate => :number, :default => 5

  # Initial connection timeout in seconds.
  config :max_connections, :validate => :number, :default => 256

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The name of a Redis list or channel.
  # TODO: change required to true
  config :keys, :validate => :array

  # Specify either list or channel.  If `redis\_type` is `list`, then we will BLPOP the
  # key.  If `redis\_type` is `channel`, then we will SUBSCRIBE to the key.
  # If `redis\_type` is `pattern_channel`, then we will PSUBSCRIBE to the key.
  # TODO: change required to true
  config :data_type, :validate => [ "list", "channel", "pattern_channel" ], :required => false

  # The number of events to return from Redis using EVAL.
  config :batch_count, :validate => :number, :default => 1

  public
  # public API
  # use to store a proc that can provide a redis instance or mock
  def add_external_redis_builder(builder) #callable
    @redis_builder = builder
    self
  end

  # use to apply an instance directly and bypass the builder
  def use_redis(instance)
    @redis = instance
    self
  end

  def new_redis_instance
    @redis_builder.call
  end

  def register
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

	@batch_offset = Random.rand(1024)
    # TODO remove after setting key and data_type to true
	
	
	if @driver == "jedis" then
		require 'org/apache/commons/commons-pool2/2.3/commons-pool2-2.3.jar'
		require 'redis/clients/jedis/2.7.2/jedis-2.7.2.jar'
		@error_handler = method(:error_handler_jedis)
	else
		require 'org/apache/commons/commons-pool2/2.3/redis-rb-cluster'
		@error_handler = method(:error_handler_redis)
	end
   
    if !@keys || !@data_type
      raise RuntimeError.new(
        "Must define queue, or key and data_type parameters"
      )
    end
    # end TODO

    @redis_builder ||= method(:internal_redis_builder)

    # just switch on data_type once
    if @data_type == 'list' || @data_type == 'dummy'
      @run_method = method(:list_runner)
      @stop_method = method(:list_stop)
    elsif @data_type == 'channel'
      @run_method = method(:channel_runner)
      @stop_method = method(:subscribe_stop)
    elsif @data_type == 'pattern_channel'
      @run_method = method(:pattern_channel_runner)
      @stop_method = method(:subscribe_stop)
    end

    # TODO(sissel, boertje): set @identity directly when @name config option is removed.
    @identity = @name != 'default' ? @name : "#{@redis_url} #{@data_type}"
    @logger.info("Registering Redis", :identity => @identity)
  end # def register

  def run(output_queue)
    @run_method.call(output_queue)
  rescue LogStash::ShutdownSignal
    # ignore and quit
  end # def run

  def stop
    @stop_method.call
  end

  # private methods -----------------------------
  private

  def batched?
    @batch_count > 1
  end

  # private
  def is_list_type?
    @data_type == 'list'
  end

  # private
  def redis_params
    {
      :host => @host,
      :port => @port,
      :db => @db,
      :password => @password.nil? ? nil : @password.value,
	  :driver => @driver,
	  :timeout => @timeout
    }
  end

  # private
  def internal_redis_builder
	if @driver == "jedis" then
		import "redis.clients.jedis.JedisCluster"
		import "redis.clients.jedis.HostAndPort"
		::JedisCluster.new(java.util.HashSet.new([HostAndPort.new(redis_params[:host],redis_params[:port])]))
	else
		::RedisCluster.new([redis_params], @max_connections)
	end
  end

  # private
  def connect
    redis = new_redis_instance
    redis
  end # def connect

  # private
  def load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    #in case count is bigger than current items in queue whole queue will be returned without extra nil values
    redis_script = <<EOF
          local i = tonumber(ARGV[1])
          local res = {}
          local length = redis.call('llen',KEYS[1])
          if length < i then i = length end
          while (i > 0) do
            local item = redis.call("lpop", KEYS[1])
            if (not item) then
              break
            end
            table.insert(res, item)
            i = i-1
          end
          return res
EOF
    @redis_script_sha = redis.script(:load, redis_script)
  end

  # private
  def queue_event(msg, output_queue)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        output_queue << event
      end
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e, :backtrace => e.backtrace);
    end
  end

  # private
  def list_stop
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end
  
  # private
  def error_handler_redis(func)
	begin
        func.call
      rescue ::Redis::BaseError => e
        @logger.warn("Redis connection problem", :exception => e)
        # Reset the redis variable to trigger reconnect
        @redis = nil
        
		return false
      end
	  
	  return true
  end
  
  def error_handler_jedis(func)
	import "redis.clients.jedis.exceptions.JedisException"
	begin
        func.call
	  rescue ::JedisException => e
		@logger.warn("Redis connection problem", :exception => e)
		# Reset the redis variable to trigger reconnect
		@redis = nil
		
		return false
	  end
	  
	  return true
  end

  # private
  def list_runner(output_queue)
    while !stop?
		if !@error_handler.call(lambda {
			@redis ||= connect
			list_listener(@redis, output_queue)
		}) then
			sleep 1
		end
    end
  end

  # private
  def list_listener(redis, output_queue)
	if batched? then
		fail = 0
	    begin
			f = @batch_offset%(@keys.length)
			for i in 1..@batch_count do
				if @driver == "jedis" then
					item = redis.lpop(@keys[f])
				else
					error,item = redis.lpop(@keys[f])
				end
				if item then
					fail = 0
					queue_event(item, output_queue) 
				else
					fail = fail + 1
					break
				end
			end
			@batch_offset += 1
		end until fail>=3
	end
  
	sampled = @keys.sample
	if @driver == "jedis" then
		item = redis.blpop(1, sampled)
	else
		item = redis.blpop(sampled, :timeout => 1)
	end
    return unless item # from timeout or other conditions

    # blpop returns the 'key' read from as well as the item result
    # we only care about the result (2nd item in the list).
    if @driver == "jedis" then
        item = item.get(item.size()-1);
    else
        item = item.last
    end
    queue_event(item, output_queue)
  end

  # private
  def subscribe_stop
    return if @redis.nil? || !@redis.connected?
    # if its a SubscribedClient then:
    # it does not have a disconnect method (yet)
	if @driver == "jedis" then
	  @redis.quit
    elsif @redis.client.is_a?(::Redis::SubscribedClient)
      @redis.client.unsubscribe
    else
      @redis.client.disconnect
    end
    @redis = nil
  end

  # private
  def redis_runner
	if @error_handler.call(lambda {
      @redis ||= connect
	}) then
      yield
	else
      # Reset the redis variable to trigger reconnect
      @redis = nil
      Stud.stoppable_sleep(1) { stop? }
      retry if !stop?
	end
  end

  # private
  def channel_runner(output_queue)
    redis_runner do
      channel_listener(output_queue)
    end
  end

  # private
  def channel_listener(output_queue)
    @redis.subscribe(@keys.sample) do |on|
      on.subscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.message do |channel, message|
        queue_event(message, output_queue)
      end

      on.unsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

  def pattern_channel_runner(output_queue)
    redis_runner do
      pattern_channel_listener(output_queue)
    end
  end

  # private
  def pattern_channel_listener(output_queue)
    @redis.psubscribe @keys.sample do |on|
      on.psubscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.pmessage do |pattern, channel, message|
        queue_event(message, output_queue)
      end

      on.punsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

# end

end end end # Redis Inputs  LogStash
