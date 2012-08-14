module ActiveRecord

  class LostConnection < WrappedDatabaseException
  end

  module ConnectionAdapters
    module NuoDB
      module Errors

        LOST_CONNECTION_MESSAGES = [/remote connection closed/i].freeze

        def lost_connection_messages
          LOST_CONNECTION_MESSAGES
        end

        CONNECTION_NOT_ESTABLISHED_MESSAGES = [/can't find broker for database/i, /no .* nodes are available for database/i]

        def connection_not_established_messages
          CONNECTION_NOT_ESTABLISHED_MESSAGES
        end

      end
    end
  end

end