require_relative "../test_helper"


class GcloudPubSubOutputTest < Test::Unit::TestCase
  DEFAULT_CONFIG = <<-EOC
    type gcloud_pubsub
    project project-test
    topic topic-test
    key key-test
    flush_interval 1
  EOC
  ReRaisedError = Class.new(RuntimeError)

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::GcloudPubSubOutput).configure(conf)
  end

  def test_configure
    d = create_driver(<<-EOC)
      type gcloud_pubsub
      project project-test
      topic topic-test
      key key-test
      flush_interval 1
    EOC

    assert_equal('project-test', d.instance.project)
    assert_equal('topic-test', d.instance.topic)
    assert_equal('key-test', d.instance.key)
    assert_equal(false, d.instance.autocreate_topic)
    assert_equal(1, d.instance.flush_interval)
    assert_equal(1000, d.instance.max_messages)
  end

  def test_autocreate_topic
    d = create_driver(<<-EOC)
      type gcloud_pubsub
      project project-test
      topic topic-test
      key key-test
      flush_interval 1
      autocreate_topic true
    EOC

    assert_equal(true, d.instance.autocreate_topic)

    chunk = Fluent::MemoryBufferChunk.new('key', 'data')
    client = mock!
    client.topic("topic-test", autocreate: true).once

    gcloud_mock = mock!.pubsub { client }
    stub(Gcloud).new { gcloud_mock }

    d.instance.instance_variable_set(:@client, client)

    d.instance.start()
  end

  def test_max_messages
    d = create_driver(DEFAULT_CONFIG)

    client = mock!
    client.name.times(2) { 'topic-test' }
    client.publish.times(2)

    pubsub_mock = mock!.topic(anything, anything) { client }
    gcloud_mock = mock!.pubsub { pubsub_mock }
    stub(Gcloud).new { gcloud_mock }

    time = Time.parse("2016-07-09 11:12:13 UTC").to_i

    # max_messages is default 1000
    1001.times do |i|
      d.emit({"a" => i}, time)
    end

    d.run
  end

  def test_re_raise_errors
    d = create_driver(DEFAULT_CONFIG)
    chunk = Fluent::MemoryBufferChunk.new('key', 'data')
    client = Object.new
    def client.publish
      raise ReRaisedError
    end
    def client.name
      'test-topic'
    end
    d.instance.instance_variable_set(:@client, client)

    assert_raises ReRaisedError do
      d.instance.write(chunk)
    end
  end
end
