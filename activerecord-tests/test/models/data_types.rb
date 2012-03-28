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

class DbTypeMigration < ActiveRecord::Migration
  def self.up
    create_table "db_types", :force => true do |t|
      t.column :sample_timestamp, :timestamp
      t.column :sample_datetime, :datetime
      t.column :sample_date, :date
      t.column :sample_time, :time
      t.column :sample_decimal, :decimal, :precision => 15, :scale => 0
      t.column :sample_small_decimal, :decimal, :precision => 3, :scale => 2
      t.column :sample_default_decimal, :decimal
      t.column :sample_float, :float
      t.column :sample_binary, :binary
      t.column :sample_boolean, :boolean
      t.column :sample_string, :string, :default => ''
      t.column :sample_integer, :integer, :limit => 5
      t.column :sample_integer_with_limit_2, :integer, :limit => 2
      t.column :sample_integer_with_limit_8, :integer, :limit => 8
      t.column :sample_integer_no_limit, :integer
      t.column :sample_integer_neg_default, :integer, :default => -1
      t.column :sample_text, :text
    end
  end

  def self.down
    drop_table "db_types"
  end
end

class DbType < ActiveRecord::Base
end
