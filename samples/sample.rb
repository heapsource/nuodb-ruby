require 'rubygems'
require 'active_record'

puts "Connecting to database..."

ActiveRecord::Base.establish_connection(
  :adapter => 'nuodb',
  :database => 'test',
  :username => 'cloud',
  :password => 'user'
)

puts "Create tables..."

def drop(name)
  begin
    ActiveRecord::Schema.drop_table(name)
  rescue
    nil
  end
end

drop(:sample_users)
drop(:sample_addrs)

ActiveRecord::Schema.define do
  create_table :sample_users do |t|
    t.string :first_name, :limit => 20
    t.string :last_name, :limit => 20
    t.string :email, :limit => 20
    t.string :user_name, :limit => 20
    t.boolean :admin
  end
  create_table :sample_addrs do |t|
    t.integer :sample_user_id
    t.string :street, :limit => 20
    t.string :city, :limit => 20
    t.string :zip, :limit => 6
  end
end

class SampleUser < ActiveRecord::Base
  has_one :addr, :class_name => 'SampleAddr'
  def to_s
    return "SampleUser(#{id}), Username: #{user_name}, Name: #{first_name} #{last_name}, #{admin ? "admin" : "member"}\n" +
    "  Address: #{addr}\n"
  end
end

class SampleAddr < ActiveRecord::Base
  belongs_to :SampleUser
  def to_s
    return "SampleAddr(#{id}:#{sample_user_id}) Street: #{street} City: #{city} Zip: #{zip}"
  end
end

puts "Create user records..."

@u = SampleUser.create do |u|
  u.first_name = "Fred"
  u.last_name = "Flintstone"
  u.email = "fredf@example.com"
  u.user_name = "fred"
  u.admin = true
end

@u.create_addr do |a|
  a.street = "301 Cobblestone Way"
  a.city = "Bedrock"
  a.zip = "00001"
end

puts "Created #{@u}"

@u = SampleUser.create do |u|
  u.first_name = "Barney"
  u.last_name = "Rubble"
  u.email = "barney@example.com"
  u.user_name = "barney"
  u.admin = false
end

@u.create_addr do |a|
  a.street = "303 Cobblestone Way"
  a.city = "Bedrock"
  a.zip = "00001"
end

puts "Created #{@u}"

puts "Print user records..."

puts "Found #{SampleUser.count} records:"
SampleUser.find do |entry|
  puts entry
end

puts "Modify user records..."

SampleUser.all.each do |entry|
  entry.first_name = entry.first_name.upcase
  entry.last_name = entry.last_name.upcase
  entry.admin = !entry.admin
  entry.addr.street = entry.addr.street.upcase
  entry.addr.save 
  entry.save
end

puts "Print user records..."

puts "Found #{SampleUser.count} records:"
SampleUser.find_each do |entry|
  puts entry
end

