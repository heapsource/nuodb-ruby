# -*- encoding: utf-8 -*-
require File.expand_path('../lib/active_record/connection_adapters/nuodb/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name = "activerecord-nuodb-adapter"
  gem.summary = %q{ActiveRecord adapter with AREL support for NuoDB.}
  gem.description = %q{An adapter for ActiveRecord and AREL to support the NuoDB distributed database backend.}
  gem.homepage = "http://www.nuodb.com/"
  gem.authors = %w(Robert Buck)
  gem.email = %w(support@nuodb.com)
  gem.license = "BSD"
  gem.version = ActiveRecord::ConnectionAdapters::Nuodb::Version::VERSION
  gem.date = '2012-07-30'

  gem.rdoc_options = %w(--charset=UTF-8)
  gem.extra_rdoc_files = %w[README.rdoc]

  gem.add_development_dependency('rake', '~> 0.9')
  gem.add_development_dependency('rdoc', '~> 3.10')
  gem.add_development_dependency('rcov', '~> 1.0')

  gem.files = `git ls-files`.split($\)
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = %w(lib)
end
