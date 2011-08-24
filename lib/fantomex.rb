module Fantomex
  VERSION = "0.0.1"

  # Represents a single queued message.
  class Message
    attr_accessor :id, :data, :retries, :run_at

    def initialize(data = {})
      case data
      when String
        @data = data
      when Hash
        @data    = data[:data]
        @retries = data[:retries]
        @run_at  = data[:run_at]
        @id      = data[:id]
      end
      @retries ||= 0
      @run_at  ||= Time.now
    end

    # Public: Reschedules this job to run in the future.  Uses exponential
    # backoff.
    #
    # Returns nothing.
    def reschedule!
      duration  = (@retries ** 4) + 5
      @retries += 1
      @run_at  += duration
    end
  end

  module Adapters
    autoload :SequelAdapter, File.expand_path("../fantomex/adapters/sequel_adapter", __FILE__)
  end

  class Adapter
    def initialize(options = {})
    end

    # Public: Adds a new Message to the queue.
    #
    # data - The String message.
    #
    # Returns a Message instance with a set id.
    def push(data)
      raise NotImplementedError
    end

    # Public: Gets the earliest Message.
    #
    # Returns a Message.
    def peek
      raise NotImplementedError
    end

    # Public: Counts the messages in the queue.
    #
    # Returns the Integer count.
    def count
      raise NotImplementedError
    end
    
    # Public: Removes a message from the queue.
    #
    # id - Integer ID of the Message.
    #
    # Returns nothing.
    def remove(id)
      raise NotImplementedError
    end

    # Public: Reschedules the Message to run at a later time.  Re-assigns
    # the time using exponential back-off.
    #
    # msg - The Message to reschedule.
    #
    # Returns the updated Message.
    def reschedule(msg)
      raise NotImplementedError
    end

    # Public: Sets up the DB schema for the queue.
    #
    # Returns nothing.
    def setup
    end
  end
end

