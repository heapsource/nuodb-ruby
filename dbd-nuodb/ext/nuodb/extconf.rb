
require 'mkmf'

if ENV["NUODB_HOME"]
   NUODB_HOME = ENV["NUODB_HOME"]
   $CFLAGS="-I#{NUODB_HOME}/include"
   $LDFLAGS="-L#{NUODB_HOME}/lib"
else
   STDERR.puts <<EOF
Your $NUODB_HOME environment variable is not set.  You may either set it and
start over or manually edit the Makefile to point to the appropriate
directories.
EOF
end

dir_config("nuodb")
have_header("nuodbcci.h")
have_library("nuodbcci")
create_makefile("nuodb/nuodb")
