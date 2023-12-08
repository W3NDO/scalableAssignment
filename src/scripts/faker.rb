require 'faker'
require 'csv'

def build_triple # build triples in batches of 10. At least 20% follow each other.
  triples = Array.new

  users = (1..10).map { Faker::Name.first_name }
  users.each do |user|
    triples << ["follows", user, (users - [user]).sample].join(",")
  end
  popular_user = users.sample
  3.times do 
    triples << ["follows", popular_user, (users - [popular_user]).sample].join(",")
  end

  [triples, users]
end

triples = []
users = []
30000.times do 
  new_triple = build_triple
  triples << new_triple[0]
  users << new_triple[1]
end

CSV.open('./datasets/fake_social_media.csv', 'w') do |file|
  file << ['predicate', 'subject', 'object']
  triples.flatten.each do |triple|
    file << triple.split(",")
  end
end

CSV.open('./datasets/fake_users.csv', 'w') do |csv|
  csv << ['Name']

  users.flatten.each { |name| csv << [name] }
end

pp users.flatten