require 'gcloud'
require 'fluent/plugin/output'

module Fluent::Plugin
  class GcloudPubSubOutput < Output
    Fluent::Plugin.register_output('gcloud_pubsub', self)

    helpers :compat_parameters

    DEFAULT_BUFFER_TYPE = "memory"

    config_param :project,            :string,  :default => nil
    config_param :topic,              :string
    config_param :key,                :string,  :default => nil
    config_param :autocreate_topic,   :bool,    :default => false

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      # In v0.14, buffer configurations are renamed.
      # see: https://github.com/fluent/fluentd/blob/master/lib/fluent/plugin/buffer.rb
      config_set_default :flush_interval,      1
      config_set_default :try_flush_interval,  0.05
      config_set_default :chunk_limit_records, 900
      config_set_default :chunk_limit_size,    9437184
      config_set_default :queue_limit_length,  64
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super
    end

    def start
      super

      pubsub = (Gcloud.new @project, @key).pubsub
      @client = pubsub.topic @topic, autocreate: @autocreate_topic
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary?
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      messages = []

      chunk.msgpack_each do |tag, time, record|
        messages << record.to_json
      end

      if messages.length > 0
        @client.publish do |batch|
          messages.each do |m|
            batch.publish m
          end
        end
      end
    rescue => e
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
      raise e
    end
  end
end
