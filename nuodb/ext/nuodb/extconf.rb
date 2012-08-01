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

require 'mkmf'

def parameter_empty?(parameter)
  parameter.nil? || parameter.empty?
end

if parameter_empty? ENV['NUODB_ROOT']
  if File.directory? '/opt/nuodb'
    dir_config('nuodb', '/opt/nuodb/include', '/opt/nuodb/lib64')
  end
else
  nuodb_root = ENV['NUODB_ROOT']
  if File.directory? nuodb_root
    nuodb_include = File.join(nuodb_root, 'include')
    nuodb_lib64   = File.join(nuodb_root, 'lib64')
    dir_config('nuodb', nuodb_include, nuodb_lib64)
  end
end

fail = false

def create_dummy_makefile
  File.open("Makefile", 'w') do |f|
    f.puts "all:"
    f.puts "install:"
  end
end

case RUBY_PLATFORM
  when /bsd/i, /darwin/i
    # extras here...
  when /linux/i
    # extras here...
  when /solaris|sunos/i
    # extras here...
  have_library('stdc++')
  $LDFLAGS << ' -m64'
  else
    puts
    puts "Unsupported platform '#{RUBY_PLATFORM}'. Supported platforms are BSD, DARWIN, and LINUX."
    create_dummy_makefile
end

if CONFIG['warnflags']
CONFIG['warnflags'].slice!(/-Wdeclaration-after-statement/)
CONFIG['warnflags'].slice!(/-Wimplicit-function-declaration/)
end

dir_config("nuodb")
have_library("NuoRemote") or raise
create_makefile('nuodb/nuodb')