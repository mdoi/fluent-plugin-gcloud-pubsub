require 'gcloud'

module Fluent
  class GcloudPubSubOutput < Fluent::BufferedOutput
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
    config_param :set_tag_to_attr,    :string,  :default => false

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super

      raise Fluent::ConfigError, "'project' must be specified." unless @project
      raise Fluent::ConfigError, "'topic' must be specified." unless @topic
      raise Fluent::ConfigError, "'key' must be specified." unless @key
    end

    def start
      super

      pubsub = (Gcloud.new @project, @key).pubsub
      @client = pubsub.topic @topic
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each do |tag, time, record|
        @client.publish do |batch|
          if @set_tag_to_attr
            batch.publish record.to_json, tag: tag
          else
            batch.publish record.to_json
          end
        end
      end
    rescue
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
    end
  end
end
