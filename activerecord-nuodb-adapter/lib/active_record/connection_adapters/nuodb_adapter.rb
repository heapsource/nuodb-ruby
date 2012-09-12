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

require 'nuodb'
require 'arel/visitors/nuodb'
require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'
require 'active_record/connection_adapters/nuodb/version'
require 'arel/visitors/bind_visitor'

module ActiveRecord

  class Base

    def self.nuodb_connection(config) #:nodoc:
      config.symbolize_keys!
      unless config[:database]
        raise ArgumentError, "No database file specified. Missing argument: database"
      end
      # supply configuration defaults
      config.reverse_merge! :host => 'localhost'
      ConnectionAdapters::NuoDBAdapter.new nil, logger, nil, config
    end

  end

  class LostConnection < WrappedDatabaseException
  end

  module ConnectionAdapters

    class NuoDBColumn < Column

      def initialize(name, default, sql_type = nil, null = true, options = {})
        @options = options.symbolize_keys
        super(name, default, sql_type, null)
        @primary = @options[:is_identity] || @options[:is_primary]
      end

      class << self

        def string_to_binary(value)
          "0x#{value.unpack("H*")[0]}"
        end

        def binary_to_string(value)
          value =~ /[^[:xdigit:]]/ ? value : [value].pack('H*')
        end

      end

      def is_identity?
        @options[:is_identity]
      end

      def is_primary?
        @options[:is_primary]
      end

      def is_utf8?
        !!(@sql_type =~ /nvarchar|ntext|nchar/i)
      end

      def is_integer?
        !!(@sql_type =~ /int/i)
      end

      def is_real?
        !!(@sql_type =~ /real/i)
      end

      def sql_type_for_statement
        if is_integer? || is_real?
          sql_type.sub(/\((\d+)?\)/, '')
        else
          sql_type
        end
      end

      def default_function
        @options[:default_function]
      end

      def table_name
        @options[:table_name]
      end

      def table_klass
        @table_klass ||= begin
          table_name.classify.constantize
        rescue StandardError, NameError, LoadError
          nil
        end
        (@table_klass && @table_klass < ActiveRecord::Base) ? @table_klass : nil
      end

      private

      def extract_limit(sql_type)
        case sql_type
          when /^smallint/i
            2
          when /^int/i
            4
          when /^bigint/i
            8
          when /\(max\)/, /decimal/, /numeric/
            nil
          else
            super
        end
      end

      def simplified_type(field_type)
        case field_type
          when /real/i then
            :float
          when /money/i then
            :decimal
          when /image/i then
            :binary
          when /bit/i then
            :boolean
          when /uniqueidentifier/i then
            :string
          when /datetime/i then
            :datetime
          when /varchar\(max\)/ then
            :text
          when /timestamp/ then
            :binary
          else
            super
        end
      end

    end #class NuoDBColumn

    class NuoDBAdapter < AbstractAdapter

      class StatementPool < ConnectionAdapters::StatementPool

        attr_reader :max, :connection

        def initialize(connection, max)
          super
          @cache = Hash.new { |h, pid| h[pid] = {} }
        end

        def each(&block)
          cache.each(&block)
        end

        def key?(key)
          cache.key?(key)
        end

        def [](key)
          cache[key]
        end

        def []=(sql, key)
          while max <= cache.size
            dealloc cache.shift.last[:stmt]
          end
          cache[sql] = key
        end

        def length;
          cache.length
        end

        def delete(key)
          dealloc cache[key][:stmt]
          cache.delete(key)
        end

        def clear
          cache.each_value do |hash|
            dealloc hash[:stmt]
          end
          cache.clear
        end

        private

        def cache
          @cache[Process.pid]
        end

        def dealloc(stmt)
          stmt.close if connection.ping
        end
      end

      def process_id
        Process.pid
      end

      attr_accessor :config, :statements

      class BindSubstitution < Arel::Visitors::NuoDB
        include Arel::Visitors::BindVisitor
      end

      def initialize(connection, logger, pool, config)
        super(connection, logger, pool)
        @visitor = Arel::Visitors::NuoDB.new self
        @config = config.clone
        # prefer to run with prepared statements unless otherwise specified
        if @config.fetch(:prepared_statements) { true }
          @visitor = Arel::Visitors::NuoDB.new self
        else
          @visitor = BindSubstitution.new self
        end
        connect!
      end

      # ABSTRACT ADAPTER #######################################

      # ADAPTER NAME ===========================================

      def adapter_name
        'NuoDB'
      end

      # FEATURES ===============================================

      def supports_migrations?
        true
      end

      def supports_primary_key?
        true
      end

      def supports_count_distinct?
        true
      end

      def supports_ddl_transactions?
        true
      end

      def supports_bulk_alter?
        false
      end

      def supports_savepoints?
        true
      end

      def supports_index_sort_order?
        true
      end

      def supports_partial_index?
        false
      end

      def supports_explain?
        false
      end

      # CONNECTION MANAGEMENT ==================================

      def reconnect!
        disconnect!
        connect!
        super
      end

      def connect!
        # todo add native method for new and initialize where init takes a hash
        @connection = ::NuoDB::Connection.createSqlConnection(
            "#{config[:database]}@#{config[:host]}", config[:schema],
            config[:username], config[:password])
        @statements = StatementPool.new(@connection, @config.fetch(:statement_limit) { 1000 })
      end

      def disconnect!
        super
        clear_cache!
        raw_connection.disconnect rescue nil
      end

      def reset!
        reconnect!
      end

      def clear_cache!
        @statements.clear
        @statements = nil
      end

      # SAVEPOINT SUPPORT ======================================

      def create_savepoint
        execute("SAVEPOINT #{current_savepoint_name}")
      end

      def rollback_to_savepoint
        execute("ROLLBACK TO SAVEPOINT #{current_savepoint_name}")
      end

      def release_savepoint
        execute("RELEASE SAVEPOINT #{current_savepoint_name}")
      end

      # EXCEPTION TRANSLATION ==================================

      protected

      LOST_CONNECTION_MESSAGES = [/remote connection closed/i].freeze

      def lost_connection_messages
        LOST_CONNECTION_MESSAGES
      end

      CONNECTION_NOT_ESTABLISHED_MESSAGES = [/can't find broker for database/i, /no .* nodes are available for database/i]

      def connection_not_established_messages
        CONNECTION_NOT_ESTABLISHED_MESSAGES
      end

      def translate_exception(exception, message)
        case message
          when /duplicate value in unique index/i
            RecordNotUnique.new(message, exception)
          when /too few values specified in the value list/i
            # defaults to StatementInvalid, so we are okay, but just to be explicit...
            super
          when *lost_connection_messages
            LostConnection.new(message, exception)
          when *connection_not_established_messages
            ConnectionNotEstablished.new(message)
          else
            super
        end
      end

      # SCHEMA DEFINITIONS #####################################

      public

      def pk_and_sequence_for(table_name)
        idcol = identity_column(table_name)
        idcol ? [idcol.name, nil] : nil
      end

      def primary_key(table_name)
        identity_column(table_name).try(:name)
      end

      def version
        self.class::VERSION
      end

      # SCHEMA STATEMENTS ######################################

      public

      def table_exists?(table_name)
        tables.include?(table_name.to_s)
      end

      def tables()
        sql = 'select tablename from system.tables where schema = ?'
        cache = statements[sql] ||= {
            :stmt => raw_connection.prepare(sql)
        }
        stmt = cache[:stmt]
        stmt.setString 1, raw_connection.getSchema
        result = stmt.executeQuery
        tables = []
        while result.next
          tables << result.getString(1)
        end
        tables
      end

      def columns(table_name, name = nil)
        sql = 'select field,datatype,length from system.fields where schema = ? and tablename = ?'
        cache = statements[sql] ||= {
            :stmt => raw_connection.prepare(sql)
        }
        stmt = cache[:stmt]

        schema_name, table_name = split_table_name table_name
        stmt.setString 1, schema_name
        stmt.setString 2, table_name

        rset = stmt.executeQuery
        cols = []
        while rset.next
          name = rset.getString(1).downcase
          # todo this was unimplemented, fix this mess
          default = nil
          sql_type = nil # TODO should come from query
          null = true # TODO should come from query
          cols << Column.new(name, default, sql_type, null)
        end
        cols
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # todo implement the remaining schema statements methods: rename columns, tables, etc...
      # todo, and these methods have to clear the cache!!!

      private

      NATIVE_DATABASE_TYPES = {
          :primary_key => 'int not null generated always primary key',
          :string => {:name => 'varchar', :limit => 255},
          :text => {:name => 'varchar', :limit => 255},
          :integer => {:name => 'integer'},
          :float => {:name => 'float', :limit => 8},
          :decimal => {:name => 'decimal'},
          :datetime => {:name => 'datetime'},
          :timestamp => {:name => 'datetime'},
          :time => {:name => 'time'},
          :date => {:name => 'date'},
          :binary => {:name => 'binary'},
          :boolean => {:name => 'boolean'},
          :char => {:name => 'char'},
          :varchar_max => {:name => 'varchar(max)'},
          :nchar => {:name => 'nchar'},
          :nvarchar => {:name => 'nvarchar', :limit => 255},
          :nvarchar_max => {:name => 'nvarchar(max)'},
          :ntext => {:name => 'ntext', :limit => 255},
          :ss_timestamp => {:name => 'timestamp'}
      }

      def split_table_name(table)
        name_parts = table.split '.'
        case name_parts.length
          when 1
            schema_name = raw_connection.getSchema
            table_name = name_parts[0]
          when 2
            schema_name = name_parts[0]
            table_name = name_parts[1]
          else
            raise "Invalid table name: #{table}"
        end
        [schema_name, table_name]
      end

      # QUOTING ################################################

      public

      def quote_column_name(column_name)
        column_name.to_s
      end

      def quoted_true
        "'true'"
      end

      def quoted_false
        "'false'"
      end

      # DATABASE STATEMENTS ####################################

      public

      def outside_transaction?
        false
      end

      def supports_statement_cache?
        true
      end

      # Begins the transaction (and turns off auto-committing).
      def begin_db_transaction()
        log('begin transaction', nil) {
          raw_connection.autocommit = false if raw_connection.autocommit?
        }
      end

      # Commits the transaction (and turns on auto-committing).
      def commit_db_transaction()
        log('commit transaction', nil) {
          raw_connection.autocommit = true unless raw_connection.autocommit?
          raw_connection.commit
        }
      end

      # Rolls back the transaction (and turns on auto-committing). Must be
      # done if the transaction block raises an exception or returns false.
      def rollback_db_transaction()
        log('rollback transaction', nil) {
          raw_connection.autocommit = true unless raw_connection.autocommit?
          raw_connection.rollback
        }
      end

      def execute(sql, name = 'SQL')
        log(sql, name) do
          cache = statements[process_id] ||= {
              :stmt => raw_connection.createStatement
          }
          stmt = cache[:stmt]
          stmt.execute(sql)
        end
      end

      def exec_query(sql, name = 'SQL', binds = [])
        log(sql, name, binds) do
          if binds.empty?

            cache = statements[process_id] ||= {
                :stmt => raw_connection.createStatement
            }
            stmt = cache[:stmt]

            stmt.execute(sql)
            genkeys = stmt.getGeneratedKeys
            row = genkeys ? next_row(genkeys) : nil
            @last_inserted_id = row ? row[0] : nil
            result = stmt.getResultSet
            if result
              names = column_names result
              rows = all_rows result
              ActiveRecord::Result.new(names, rows)
            else
              nil
            end

          else

            cache = statements[sql] ||= {
                :stmt => raw_connection.prepare(sql)
            }
            stmt = cache[:stmt]
            binds.to_enum.with_index(1).each { |bind, column|
              value = bind[1]
              case value
                when String
                  stmt.setString column, value
                when Integer
                  stmt.setInteger column, value
                when Fixnum
                  stmt.setInteger column, value
                when Float
                  stmt.setDouble column, value
                when TrueClass
                  stmt.setBoolean column, true
                when FalseClass
                  stmt.setBoolean column, false
                when Time
                  stmt.setTime column, value
                else
                  raise "don't know how to bind #{value.class} to parameter #{column}"
              end
            }

            stmt.execute

            genkeys = stmt.getGeneratedKeys
            row = genkeys ? next_row(genkeys) : nil
            @last_inserted_id = row ? row[0] : nil

            result = stmt.getResultSet

            if result
              names = column_names result
              rows = all_rows result
              ActiveRecord::Result.new(names, rows)
            else
              nil
            end

          end
        end
      end

      def last_inserted_id(result)
        @last_inserted_id
      end

      protected

      def select(sql, name = nil, binds = [])
        exec_query(sql, name, binds).to_a
      end

      protected

      def select_rows(sql, name = nil)
        rows = exec_query(sql, name).rows
      end

      private

      def column_names (result)
        return [] if result.nil?
        names = []
        meta = result.getMetaData
        count = meta.getColumnCount
        (1..count).each { |i|
          names << meta.getColumnName(i).downcase
        }
        names
      end

      def all_rows(result)
        rows = []
        while (row = next_row(result)) != nil
          rows << row
        end
        rows
      end

      def next_row(result)
        return nil if result.nil?
        if result.next
          meta = result.getMetaData
          count = meta.getColumnCount
          row = []
          (1..count).each { |i|
            type = meta.getType(i)
            case type
              when :SQL_INTEGER
                row << result.getInteger(i)
              when :SQL_DOUBLE
                row << result.getDouble(i)
              when :SQL_STRING
                row << result.getString(i)
              when :SQL_DATE
                row << result.getDate(i)
              when :SQL_TIME
                row << result.getTime(i)
              when :SQL_TIMESTAMP
                row << result.getTimestamp(i)
              when :SQL_CHAR
                row << result.getChar(i)
              when :SQL_BOOLEAN
                row << result.getBoolean(i)
              else
                raise "unknown type #{type} for column #{i}"
            end
          }
          row
        else
          nil
        end
      end

    end

  end

end
