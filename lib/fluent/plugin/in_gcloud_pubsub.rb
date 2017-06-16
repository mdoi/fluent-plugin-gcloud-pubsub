require 'gcloud'
require 'fluent/plugin/input'
require 'fluent/plugin/parser'

module Fluent::Plugin
  class GcloudPubSubInput < Input
    Fluent::Plugin.register_input('gcloud_pubsub', self)

    helpers :parser, :thread

    config_param :tag,                :string
    config_param :project,            :string,  :default => nil
    config_param :topic,              :string
    config_param :subscription,       :string
    config_param :key,                :string,  :default => nil
    config_param :pull_interval,      :integer, :default => 5
    config_param :max_messages,       :integer, :default => 100
    config_param :return_immediately, :bool,    :default => true

    config_section :parse do
      config_set_default :@type, 'json'
    end

    def configure(conf)
      super

      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = parser_create
    end

    def start
      super

      pubsub = (Gcloud.new @project, @key).pubsub
      topic = pubsub.topic @topic
      @client = topic.subscription @subscription
      @stop_subscribing = false
      @subscribe_thread = thread_create(:in_gcloud_pubsub_input, &method(:subscribe))
    end

    def shutdown
      super

      @stop_subscribing = true
      @subscribe_thread.join
    end

    private

    def subscribe
      until @stop_subscribing
        messages = @client.pull max: @max_messages, immediate: @return_immediately

        if messages.length > 0
          es = parse_messages(messages)
          unless es.empty?
            begin
              router.emit_stream(@tag, es)
            rescue
              # ignore errors. Engine shows logs and backtraces.
            end
            @client.acknowledge messages
            log.debug "#{messages.length} message(s) processed"
          end
        end

        if @return_immediately
          sleep @pull_interval
        end
      end
    rescue
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
    end

    def parse_messages(messages)
      es = Fluent::MultiEventStream.new
      messages.each do |m|
        convert_line_to_event(m.message.data, es)
      end
      es
    end

    def convert_line_to_event(line, es)
      line.chomp!  # remove \n
      @parser.parse(line) { |time, record|
        if time && record
          es.add(time, record)
        else
          log.warn "pattern not match: #{line.inspect}"
        end
      }
    rescue => e
      log.warn line.dump, :error => e.to_s
      log.debug_backtrace(e.backtrace)
    end
  end
end
