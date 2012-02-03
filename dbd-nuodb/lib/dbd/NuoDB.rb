#--
# 
#++

begin
    require 'rubygems'
    gem 'nuodb'
    gem 'dbi'
rescue LoadError => e
end

require 'dbi'
require "nuodb"

module DBI
    module DBD
        #
        # DBD::NuoDB - Database Driver for the NuoDB database system.
        #
        # Requires DBI and the 'nuodb' gem or package to work.
        #
        module NuoDB
            VERSION          = "0.1.0"
            DESCRIPTION      = "NuoDB DBI DBD, Leverages 'nuodb' low-level driver"

            # 
            # See DBI::TypeUtil#convert for more information.
            #
            def self.driver_name
                "NuoDB"
            end
        end
    end
end

require 'dbd/nuodb/driver'
require 'dbd/nuodb/database'
require 'dbd/nuodb/statement'
