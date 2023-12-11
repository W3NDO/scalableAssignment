require 'faker'
require 'csv'

def build_triple # build triples in batches of 10. At least 20% follow each other.
  triples = Array.new

  # users = (1..10).map { 
  #   {:key => :name => Faker::Name.first_name, :age => rand(12..100)} 
  # }
  users = []
  10.times do 
    name = Faker::Name.first_name
    age = rand(12..100)
    key = "#{name}_#{age}"
    users << { :key => key, :name => name, :age => age }
  end

  users.each do |user|
    followed = (users - [user]).sample
    triples << ["follows", "#{user[:key]}", "#{followed[:key]}"].join(",")
  end
  popular_user = users.sample
  3.times do 
    triples << [ "follows", popular_user[:key], ((users - [popular_user]).sample)[:key] ].join(",")
  end

  [triples, users]
end

triples = []
users = []
3.times do 
  new_triple = build_triple
  triples << new_triple[0]
  users << new_triple[1]
end

pp users
pp triples

CSV.open('./datasets/fake_social_media2.csv', 'w') do |file|
  file << ['predicate', 'subject', 'object']
  triples.flatten.each do |triple|
    file << triple.split(",")
  end
end

CSV.open('./datasets/fake_users2.csv', 'w') do |csv|
  csv << ['Name','Age','Username']

  users.flatten.each { |user| csv << [user[:name],user[:age],user[:key]] }
end
