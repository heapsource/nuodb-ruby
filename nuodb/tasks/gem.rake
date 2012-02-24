require 'rubygems/package_task'
require 'hoe'

HOE = Hoe.spec 'nuodb' do
  self.author         = ['NuoDB, Inc.']
  self.email          = ['info@nuodb.com']
  self.need_tar       = false
  self.need_zip       = false

  spec_extras[:required_ruby_version] = Gem::Requirement.new('>= 1.8.6')

  spec_extras[:extensions] = ['ext/nuodb/extconf.rb']
  clean_globs << "tmp" << "lib/nuodb/nuodb.so"

  extra_dev_deps << ['rake-compiler', "~> 0.7.0"]
end

file "#{HOE.spec.name}.gemspec" => ['Rakefile', 'tasks/gem.rake'] do |t|
  puts "Generating #{t.name}"
  File.open(t.name, 'w') { |f| f.puts HOE.spec.to_yaml }
end

desc "Generate or update the standalone gemspec file for the project"
task :gemspec => ["#{HOE.spec.name}.gemspec"]
