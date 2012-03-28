#
# Copyright (c) 2012, NuoDB, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of NuoDB, Inc. nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

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
