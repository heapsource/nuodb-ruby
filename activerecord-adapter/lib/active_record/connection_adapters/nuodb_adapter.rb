require 'active_record/connection_adapters/abstract_adapter'
require 'active_support/core_ext/object/blank'
require 'active_record/connection_adapters/statement_pool'

gem 'nuodb', '~> 0.1.0'
require 'nuodb'

module ActiveRecord

  module ConnectionHandling
    # Establishes a connection to the database that's used by all Active Record objects.
    def nuodb_connection(config)
      config[:username] = 'root' if config[:username].nil?

      if Mysql2::Client.const_defined? :FOUND_ROWS
        config[:flags] = Mysql2::Client::FOUND_ROWS
      end

      client = Mysql2::Client.new(config.symbolize_keys)
      options = [config[:host], config[:username], config[:password], config[:database], config[:port], config[:socket], 0]
      ConnectionAdapters::NuoDBAdapter.new(client, logger, options, config)
    end
  end

  module ConnectionAdapters

    class NuoDBAdapter < AbstractAdapter

      class Column < Column # :nodoc:
        def adapter
          NuoDBAdapter
        end
      end

      ADAPTER_NAME = 'NuoDB'

      def initialize(connection, logger, connection_options, config)
        super
        configure_connection
      end

      def supports_explain?
        true
      end

      # HELPER METHODS ===========================================

      def each_hash(result) # :nodoc:
        if block_given?
          result.each(:as => :hash, :symbolize_keys => true) do |row|
            yield row
          end
        else
          to_enum(:each_hash, result)
        end
      end

      def new_column(field, default, type, null, collation) # :nodoc:
        Column.new(field, default, type, null, collation)
      end

      def error_number(exception)
        exception.error_number if exception.respond_to?(:error_number)
      end

      # QUOTING ==================================================

      def quote_string(string)
        @connection.escape(string)
      end

      def substitute_at(column, index)
        Arel.sql "\0"
      end

      # CONNECTION MANAGEMENT ====================================

      def active?
        return false unless @connection
        @connection.ping
      end

      def reconnect!
        disconnect!
        connect
      end

      # Disconnects from the database if already connected.
      # Otherwise, this method does nothing.
      def disconnect!
        unless @connection.nil?
          @connection.close
          @connection = nil
        end
      end

      def reset!
        disconnect!
        connect
      end
    end
  end
end
