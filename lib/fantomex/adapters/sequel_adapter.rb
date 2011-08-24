require 'sequel'

module Fantomex
  module Adapters
    class SequelAdapter < Adapter
      attr_reader :client

      def initialize(options = {})
        @client = case options
          when Hash, String     then Sequel.connect(options)
          when Sequel::Database then options
          else
            raise ArgumentError, "Invalid options: #{options.inspect}"
        end
        @table = @client[:messages]
      end

      # Public: Adds a new Message to the queue.
      #
      # data - The String message.
      #
      # Returns a Message instance with a set id.
      def push(data)
        msg = case data
          when String  then Message.new(data)
          when Message then data
          else
            raise ArgumentError, "Invalid message: #{data.inspect}"
        end
        msg.id = @table << to_row(msg)
        msg
      end

      # Public: Gets the earliest Message.
      #
      # Returns a Message.
      def peek
        from_row @table.select(:rowid, :data, :retries, :run_at).order(:run_at).first
      end

      # Public: Counts the messages in the queue.
      #
      # Returns the Integer count.
      def count
        @table.count
      end
      
      # Public: Removes a message from the queue.
      #
      # id - Integer ID of the Message.
      #
      # Returns nothing.
      def remove(id)
        @table.where(:rowid => id.to_i).delete
      end

      # Public: Reschedules the Message to run at a later time.  Re-assigns
      # the time using exponential back-off.
      #
      # msg - The Message to reschedule.
      #
      # Returns the updated Message.
      def reschedule(msg)
        msg.reschedule!
        @table.where(:rowid => msg.id).update(to_row(msg))
        msg
      end

      # Public: Sets up the DB schema for the queue.
      #
      # Returns nothing.
      def setup
        @client.transaction do
          @client.execute "CREATE TABLE IF NOT EXISTS messages (
            data TEXT,
            retries INTEGER,
            run_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
          @client.execute "CREATE INDEX IF NOT EXISTS messages_by_run_at ON messages (run_at)"
        end
      end

      def to_row(msg)
        {:data => msg.data, :retries => msg.retries, :run_at => msg.run_at.utc}
      end

      def from_row(row)
        return nil if !row
        Message.new row.merge(:id => row[:rowid])
      end
    end
  end
end


