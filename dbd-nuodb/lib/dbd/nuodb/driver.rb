module DBI::DBD::NuoDB

  class Driver < DBI::BaseDriver

      def default_user
          ['', nil]
      end

      #
      # Connect to the database. DBD Required.
      #
      def connect(dbname, user, auth, attr)
      end
      
      #
      # Disconnect all database handles. DBD Required.
      #
      def disconnect_all
      end

      def data_sources
      end

    end

end
