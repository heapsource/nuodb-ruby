#! /usr/local/bin/ruby

require "test/unit"
require 'ostruct'
require 'nuodb'

CONFIG = OpenStruct.new
CONFIG.host = ENV['NUODB_HOST'] || 'localhost'
CONFIG.port = ENV['NUODB_PORT'] || '9999'
CONFIG.user = ENV['NUODB_USER'] || 'root'
CONFIG.pass = ENV['NUODB_PASS'] || ''
CONFIG.database = ENV['NUODB_DATABASE'] || 'test'

class TC_Nuodb < Test::Unit::TestCase

  def setup()
    @host = CONFIG.host
    @user = CONFIG.user
    @pass = CONFIG.pass
    @db   = CONFIG.database
  end

  def teardown()
  end

  def test_init()
    assert_nothing_raised{@m = Mysql.init}
    assert_nothing_raised{@m.close}
  end

end

