module DBI::DBD::NuoDB

    class Database < DBI::BaseDatabase

        # REQUIRED METHODS TO IMPLEMENT

        def disconnect
            raise NotImplementedError
        end

        def ping
            raise NotImplementedError
        end

        def tables
            raise NotImplementedError
        end

        def columns(table)
            raise NotImplementedError
        end

        def prepare(statement)
            raise NotImplementedError
        end

        # OPTIONAL METHODS TO IMPLEMENT

        def commit
            raise NotImplementedError
        end

        def rollback
            raise NotImplementedError
        end

        # OTHER METHODS

        def do(stmt, *bindvars)
        end

        #
        # Set an attribute on the database handle.
        #
        def []=(attr, value)
        end

    end
end
