require 'jdbc/nuodb'

config = {
  :username => 'cloud',
  :password => 'user',
  :adapter  => 'jdbc',
  :driver   => 'com.nuodb.jdbc.Driver',
  :url      => 'jdbc:com.nuodb://localhost/test'
}

ActiveRecord::Base.establish_connection(config)
