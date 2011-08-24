require 'test/unit'
require File.expand_path('../../lib/fantomex', __FILE__)

class SequelAdapterTest < Test::Unit::TestCase
  def setup
    @adapter = Fantomex::Adapters::SequelAdapter.new \
      :adapter  => 'sqlite',
      :database => ":memory:"
    @adapter.setup
  end

  def test_adding_a_message
    msg = @adapter.push 'abc'
    assert msg.id > 0
    assert_equal 'abc', msg.data
    assert_equal 0,     msg.retries
  end

  def test_peek
    assert_nil @adapter.peek
    msg = @adapter.push 'abc'
    assert_equal msg.id, @adapter.peek.id
  end

  def test_count
    assert_equal 0, @adapter.count
    @adapter.push 'abc'
    assert_equal 1, @adapter.count
  end

  def test_reschedule
    msg = @adapter.push 'abc'
    start = msg.run_at

    assert_equal start, @adapter.peek.run_at

    msg.reschedule!
    assert_equal start+5, msg.run_at
    assert_equal start, @adapter.peek.run_at

    @adapter.reschedule @adapter.peek
    assert_equal start+5, @adapter.peek.run_at
  end

  def test_remove
    assert_nil @adapter.peek
    msg = @adapter.push 'abc'
    assert_equal msg.id, @adapter.peek.id

    @adapter.remove msg.id
    assert_nil @adapter.peek
  end
end
