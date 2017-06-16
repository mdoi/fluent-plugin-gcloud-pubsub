require_relative "../test_helper"
require 'fluent/test/driver/input'

class GcloudPubSubInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::GcloudPubSubInput).configure(conf)
  end

  def test_configure
    d = create_driver(<<-EOC)
      type gcloud_pubsub
      tag test
      project project-test
      topic topic-test
      subscription subscription-test
      key key-test
      max_messages 1000
      return_immediately true
      pull_interval 2
      format json
    EOC

    assert_equal('test', d.instance.tag)
    assert_equal('project-test', d.instance.project)
    assert_equal('topic-test', d.instance.topic)
    assert_equal('subscription-test', d.instance.subscription)
    assert_equal('key-test', d.instance.key)
    assert_equal(1000, d.instance.max_messages)
    assert_equal(true, d.instance.return_immediately)
  end
end
