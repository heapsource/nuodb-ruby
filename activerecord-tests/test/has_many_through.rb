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

class CreateRbac < ActiveRecord::Migration
  def self.up
    create_table :role_assignments do |t| 
      t.column :role_id, :integer
      t.column :user_id, :integer
    end

    create_table :roles do |t| 
      t.column :name, :string
      t.column :description, :string
    end 

    create_table :permission_groups do |t|
      t.column :right_id, :integer
      t.column :role_id, :integer
    end 

    create_table :rights do |t| 
      t.column :name, :string
      t.column :controller_name, :string
      t.column :actions, :string
      t.column :hours, :float, :null => false
    end
  end

  def self.down
    drop_table :role_assignments
    drop_table :roles
    drop_table :permission_groups
    drop_table :rights
  end
end

class Right < ActiveRecord::Base
  has_many :permission_groups, :dependent => :destroy
  has_many :roles, :through => :permission_groups
end

class Role < ActiveRecord::Base
  has_many :permission_groups, :dependent => :destroy
  has_many :rights, :through => :permission_groups
  has_many :role_assignments, :dependent => :destroy
end

class PermissionGroup < ActiveRecord::Base
  belongs_to :right
  belongs_to :role
end

class RoleAssignment < ActiveRecord::Base
  belongs_to :user
  belongs_to :role
end

module HasManyThroughMethods
  def setup
    CreateRbac.up
  end

  def teardown
    CreateRbac.down
  end

  def test_has_many_through
    admin_role    = Role.create( {:name => "Administrator", :description => "System defined super user - access to right and role management."} )
    admin_role.save

    assert_equal(0, admin_role.rights.sum(:hours))

    role_rights   = Right.create( {:name => "Administrator - Full Access To Roles", :actions => "*", :controller_name => "Admin::RolesController", :hours => 0} )
    right_rights  = Right.create( {:name => "Administrator - Full Access To Rights", :actions => "*", :controller_name => "Admin::RightsController", :hours => 1.5} )

    admin_role.rights << role_rights
    admin_role.rights << right_rights
    admin_role.save

    assert_equal(1.5, admin_role.rights.sum(:hours))
  end
end
