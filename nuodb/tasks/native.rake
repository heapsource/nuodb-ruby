# use rake-compiler for building the extension
require 'rake/extensiontask'

Rake::ExtensionTask.new('nuodb', HOE.spec) do |ext|
  ext.lib_dir = "lib/nuodb"
  ext.cross_compiling do |gemspec|
    gemspec.post_install_message = <<-POST_INSTALL_MESSAGE
You have installed the binary version of #{gemspec.name}.
    POST_INSTALL_MESSAGE
  end
end

# ensure things are compiled prior testing
task :test => [:compile]
