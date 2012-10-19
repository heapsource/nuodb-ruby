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

require 'fileutils'
require 'pathname'
require 'mkmf'
require 'find'

nuodb_include = nil
nuodb_lib64 = nil

def dylib_extension
  case RUBY_PLATFORM
    when /bsd/i, /darwin/i
      # extras here...
      'dylib'
    when /linux/i, /solaris|sunos/i
      # extras here...
      'so'
    else
      puts "Unsupported platform '#{RUBY_PLATFORM}'. Supported platforms are BSD, DARWIN, SOLARIS, and LINUX."
      raise
  end
end

def nuodb_home?(path)
  unless path.nil?
    incl = File.join(path, 'include')
    libs = File.join(path, 'lib64')
    if File.directory? incl and File.directory? libs
      [incl, libs]
    else
      [nil, nil]
    end
  end
end

def nuodb_srcs?(path)
  if File.directory? path
    entries = Dir.entries path
    if entries.include? 'CMakeLists.txt'
      remote_dir = File.join(path, 'Remote')
      dylib_paths = []
      Find.find(remote_dir) do |file|
        if File.file? file
          basename = "libNuoRemote.#{dylib_extension}"
          dylib_paths << file unless file !~ /#{basename}/
        end
      end
      [File.join(path, 'Remote'), File.dirname(dylib_paths[0])] unless dylib_paths.length == 0
    else
      pathname = Pathname.new path
      nuodb_srcs? File.expand_path("..", path) unless pathname.root?
    end
  end
end

if nuodb_include.nil? and nuodb_lib64.nil?
  nuodb_include, nuodb_lib64 = nuodb_home? ENV['NUODB_ROOT']
end

# Like other package managers, gems do not install from the source directory
# even if they are installed from a local .gem file. Because of this we need
# to provide a fake NUODB_ROOT when building the gems within our source tree
# so that the installation process can find build artifacts and sources; the
# section below handles this special case so that we can run our test suites.

if nuodb_include.nil? and nuodb_lib64.nil?
  nuodb_include, nuodb_lib64 = nuodb_srcs? ENV['NUODB_ROOT']
end

# Lastly we fall back to detecting the location of installed product against
# which we compile and link.

if nuodb_include.nil? and nuodb_lib64.nil?
  nuodb_include, nuodb_lib64 = nuodb_home? '/opt/nuodb'
end

def dir_exists? (path)
  !path.nil? and File.directory? path
end

unless dir_exists? nuodb_include and dir_exists? nuodb_lib64
  puts
  puts "Neither NUODB_ROOT is set, nor is NuoDB installed to /opt/nuodb platform. Please set NUODB_ROOT to refer to NuoDB installation directory."
  exit(false)
end

dir_config('nuodb', nuodb_include, nuodb_lib64)
have_library("NuoRemote") or raise

if have_header('stdint.h') then
  $CPPFLAGS << " -DHAVE_STDINT_H"
end

if CONFIG['warnflags']
  CONFIG['warnflags'].slice!(/-Wdeclaration-after-statement/)
  CONFIG['warnflags'].slice!(/-Wimplicit-function-declaration/)
end

case RUBY_PLATFORM
  when /bsd/i, /darwin/i, /linux/i, /solaris|sunos/i
    case RUBY_PLATFORM
      when /bsd/i, /darwin/i
        # extras here...
        $LDFLAGS << " -Xlinker -rpath -Xlinker #{nuodb_lib64}"
      when /linux/i
        # extras here...
        $LDFLAGS << " -Wl,-rpath,'$$ORIGIN'"
      when /solaris|sunos/i
        # extras here...
        have_library('stdc++')
        $LDFLAGS << " -Wl,-rpath,'$$ORIGIN' -m64"
      else
        puts
    end
  else
end

create_makefile('nuodb/nuodb')
