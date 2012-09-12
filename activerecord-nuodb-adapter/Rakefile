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

require 'rubygems'
require 'rake'
require 'rake/clean'
require 'rake/testtask'
require 'rdoc/task'
require 'date'

#############################################################################
#
# Helper functions
#
#############################################################################

def name
  @name ||= Dir['*.gemspec'].first.split('.').first
end

def version
  require File.expand_path('../lib/active_record/connection_adapters/nuodb/version', __FILE__)
  ActiveRecord::ConnectionAdapters::NuoDB::VERSION
end

def rubyforge_project
  name
end

def date
  Date.today.to_s
end

def gemspec_file
  "#{name}.gemspec"
end

def gem_file
  "#{name}-#{version}.gem"
end

def replace_header(head, header_name)
  head.sub!(/(\.#{header_name}\s*= ').*'/) { "#{$1}#{send(header_name)}'" }
end


def so_ext
  case RUBY_PLATFORM
    when /bsd/i, /darwin/i
      # extras here...
      @sh_extension = 'bundle'
    when /linux/i
      # extras here...
      @sh_extension = 'so'
    when /solaris|sunos/i
      # extras here...
      @sh_extension = 'so'
    else
      puts
      puts "Unsupported platform '#{RUBY_PLATFORM}'. Supported platforms are BSD, DARWIN, and LINUX."
      exit
  end
end

#############################################################################
#
# Standard tasks
#
#############################################################################

CLEAN.include('pkg')

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "#{name} #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

Rake::TestTask.new do |t|
  t.libs << 'test'
  t.test_files = FileList['test/test*.rb']
end

task :default => :test

#############################################################################
#
# Packaging tasks
#
#############################################################################

GEM_NAME = "activerecord-nuodb-adapter"

desc "Build #{gem_file} into the pkg directory"
task :build do
  sh "mkdir -p pkg"
  sh "gem build #{gemspec_file}"
  sh "mv #{gem_file} pkg"
end

task :install => :build do
  sh %{gem install pkg/#{GEM_NAME}-#{ActiveRecord::ConnectionAdapters::NuoDB::VERSION} --no-rdoc --no-ri}
end

task :uninstall do
  sh %{gem uninstall #{GEM_NAME} -x -v #{ActiveRecord::ConnectionAdapters::NuoDB::VERSION}}
end
