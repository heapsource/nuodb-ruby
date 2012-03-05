#! /usr/local/bin/ruby

require "test/unit"
require 'ostruct'
require 'nuodb'

CONFIG = OpenStruct.new
CONFIG.database = ENV['NUODB_DATABASE'] || 'test@localhost'
CONFIG.username = ENV['NUODB_USERNAME'] || 'cloud'
CONFIG.password = ENV['NUODB_PASSWORD'] || 'user'

class TC_Nuodb < Test::Unit::TestCase

  def setup()
    @database = CONFIG.database
    @username = CONFIG.username
    @password = CONFIG.password
  end

  def teardown()
  end

  def test_version()
    @env = Nuodb::SqlEnvironment.createSqlEnvironment
    @con = @env.createSqlConnection @database, @username, @password
    @dmd = @con.getMetaData
    assert_equal '%%PRODUCT_VERSION%%', @dmd.getDatabaseVersion
  end

  def test_create_statement()
    @env = Nuodb::SqlEnvironment.createSqlEnvironment
    @con = @env.createSqlConnection @database, @username, @password
    @stmt = @con.createStatement
    assert_not_nil @stmt
  end

  # SqlResultSetWrapper
  # TODO bool next();
  # TODO size_t getColumnCount() const;
  # TODO SqlColumnMetaData & getMetaData(size_t column) const;
  # TODO int32_t getInteger(size_t column) const;
  # TODO double getDouble(size_t column) const;
  # TODO char const * getString(size_t column) const;

  # SqlColumnMetaDataWrapper
  # TODO char const * getColumnName() const;
  # TODO SqlType getType() const;

  # SqlPreparedStatementWrapper
  # TODO void setInteger(size_t index, int32_t value);
  # TODO void setDouble(size_t index, double value);
  # TODO void setString(size_t index, char const * value);
  # TODO void execute();
  # TODO SqlResultSet & executeQuery();

  # SqlConnectionWrapper
  # TODO void setAutoCommit(bool autoCommit = true);
  # TODO bool hasAutoCommit() const;
  # TODO void commit();
  # TODO void rollback();

end

