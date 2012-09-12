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

require File.expand_path('../lib/active_record/connection_adapters/nuodb/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name = "activerecord-nuodb-adapter"
  gem.summary = %q{ActiveRecord adapter with AREL support for NuoDB.}
  gem.description = %q{An adapter for ActiveRecord and AREL to support the NuoDB distributed database backend.}
  gem.homepage = "http://www.nuodb.com/"
  gem.authors = %w(Robert Buck)
  gem.email = %w(support@nuodb.com)
  gem.license = "BSD"
  gem.version = ActiveRecord::ConnectionAdapters::NuoDB::VERSION
  gem.date = '2012-07-30'

  gem.rdoc_options = %w(--charset=UTF-8)
  gem.extra_rdoc_files = %w[README.rdoc]

  gem.add_dependency('activerecord', '~> 3.2.8')
  gem.add_development_dependency('rake', '~> 0.9')
  gem.add_development_dependency('rdoc', '~> 3.10')
  gem.add_dependency('nuodb', '~> 0.2.0')

  gem.files = `git ls-files`.split($\)
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = %w(lib)
end
