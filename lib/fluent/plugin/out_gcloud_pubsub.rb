require 'gcloud'
require 'fluent/output'

module Fluent
  class GcloudPubSubOutput < BufferedOutput
    Fluent::Plugin.register_output('gcloud_pubsub', self)

    config_set_default :buffer_type,                'lightening'
    config_set_default :flush_interval,             1
    config_set_default :try_flush_interval,         0.05
    config_set_default :buffer_chunk_records_limit, 900
    config_set_default :buffer_chunk_limit,         9437184
    config_set_default :buffer_queue_limit,         64

    config_param :project,            :string,  :default => nil
    config_param :topic,              :string,  :default => nil
    config_param :key,                :string,  :default => nil
    config_param :autocreate_topic,   :bool,    :default => false
    config_param :max_messages,       :integer, :default => 1000

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super

      raise Fluent::ConfigError, "'topic' must be specified." unless @topic
    end

    def start
      super

      pubsub = (Gcloud.new @project, @key).pubsub
      @client = pubsub.topic @topic, autocreate: @autocreate_topic
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      messages = []

      chunk.msgpack_each do |tag, time, record|
        messages << record.to_json
      end

      if messages.length > 0
        messages.each_slice(@max_messages).each do |msg|
          publish msg
        end
      end
    rescue => e
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
      raise e
    end

    def publish(messages)
      log.debug "send message topic:#{@client.name} length:#{messages.length.to_s}"
      @client.publish do |batch|
        messages.each do |m|
          batch.publish m
        end
      end
    end
  end
end
