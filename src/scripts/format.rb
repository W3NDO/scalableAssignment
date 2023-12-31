require 'csv'

def process_csv(input_file)
  # Initialize arrays to store data
  first_and_third_column = ["subject, object"]
  reordered_columns = ["predicate, subject, object"]
  line = 0
  # Read CSV file
  CSV.foreach(input_file, headers: false) do |row|
    # Extract data from each row
    first_column = row[0]
    second_column = row[1]
    third_column = row[2]
    line +=1 

    # Process for the first output file
    first_and_third_column << "#{first_column},\"#{third_column}\""

    # Process for the second output file (reordered columns)
    reordered_columns << "#{second_column},#{first_column},\"#{third_column}\""
    pp line
  end

  # Write to the first output file
  File.open('./datasets/tetris_nodes.csv', 'w') do |file|
    file.puts(first_and_third_column)
  end

  # Write to the second output file
  File.open('./datasets/tetris_edge_definition.csv', 'w') do |file|
    file.puts(reordered_columns)
  end
end
# Replace 'input.csv' with your actual CSV file names 

csv_input_file = File.open('/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/tetris-dataset-triples.csv', 'r+')
process_csv(csv_input_file)
