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

class CreateEntries < ActiveRecord::Migration
  def self.up
    create_table "entries", :force => true do |t|
      t.column :title, :string, :limit => 100
      t.column :updated_on, :datetime
      t.column :content, :text
      t.column :rating, :decimal, :precision => 10, :scale => 2
      t.column :user_id, :integer
    end
  end

  def self.down
    drop_table "entries"
  end
end

class Entry < ActiveRecord::Base
  belongs_to :user

  def to_param
    "#{id}-#{title.gsub(/[^a-zA-Z0-9]/, '-')}"
  end
end

class CreateUsers < ActiveRecord::Migration
  def self.up
    create_table "users_table", :force => true do |t|
      t.column :login, :string, :limit => 100
    end
  end

  def self.down
    drop_table "users_table"
  end
end

class User < ActiveRecord::Base
  set_table_name "users_table"
  has_many :entries
end

