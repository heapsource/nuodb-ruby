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

end

