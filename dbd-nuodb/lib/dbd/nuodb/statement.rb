module DBI::DBD::NuoDB

    class Statement < DBI::BaseStatement

        #
        # Bind a parameter to a statement. DBD Required.
        #
        def bind_param(param, value, attribs)
        end

        #
        # Execute the statement with known binds. DBD Required.
        #
        def execute
        end

        #
        # Close the statement and any result cursors. DBD Required.
        #
        def finish
        end

        #
        # Fetch the next row in the result set. DBD Required.
        #
        def fetch
        end

        #
        # Provides result-set column information as an array of hashes. DBD Required.
        #
        def column_info
        end

        # 
        # Optional
        #
        def fetch_scroll(direction, offset)
        end

        # 
        # Optional
        #
        def []=(attr, value)
        end

    end
end
