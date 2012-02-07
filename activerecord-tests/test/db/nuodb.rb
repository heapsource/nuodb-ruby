NUODB_CONFIG = {
  :username => 'cloud',
  :password => 'user',
  :adapter  => 'nuodb',
  :database => 'test',
  :host     => 'localhost',
  :reconnect => true
}
ActiveRecord::Base.establish_connection(NUODB_CONFIG)

