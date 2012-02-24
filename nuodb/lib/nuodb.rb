# load the binary library
# support multiple ruby version (fat binaries under windows)
begin
  RUBY_VERSION =~ /(\d+.\d+)/
  require "nuodb/#{$1}/nuodb"
rescue LoadError
  require 'nuodb/nuodb'
end

# load the version definition
require 'nuodb/version'
