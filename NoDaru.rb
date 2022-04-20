#!/usr/bin/env ruby

require 'csv'
require 'time'
require "date"

# dataset holds the cleaned CSV contents
$dataset = []
$starting = Time.now

# Unless specified, the default filename is US_Accidents_data.csv
def read_csv(filename = "US_Accidents_data.csv")

    $starting = Time.now

    puts("Loading and cleaning input data set:")
    puts("************************************")

    puts("[#{Time.now - $starting}] Starting Script")
    puts("[#{Time.now - $starting}] Loading #{filename}")
    col_names = []
    # Read only the first line from CSV: contains all the column names
    begin
        CSV.foreach(filename, :encoding => 'UTF-8') do |names|
            # Append a new col that will contain the accidents month and year
            names.append("Month")
            names.append("Year")
            col_names = names
            break
        end
    
    rescue Errno::ENOENT
        puts("#{filename} not found\nExiting program")
        exit()
    end

    # Get the index for cols for future reference. Also global variables
    $id = col_names.find_index("ID")
    $severity = col_names.find_index("Severity")
    $zipcpde = col_names.find_index("Zipcode")
    $city = col_names.find_index("City")
    $temperature = col_names.find_index("Temperature(F)")
    $humidity = col_names.find_index("Humidity(%)")
    $start_time = col_names.find_index("Start_Time")
    $end_time = col_names.find_index("End_Time")
    $visibility = col_names.find_index("Visibility(mi)")
    $weather_condition = col_names.find_index("Weather_Condition")
    $country = col_names.find_index("Country")
    $distance = col_names.find_index("Distance(mi)")
    $state = col_names.find_index("State")
    $month = col_names.find_index("Month")
    $year = col_names.find_index("Year")

    puts("[#{Time.now - $starting}] Performing Data Clean Up")

    # Read the entire CSV and perform cleaning as it reads each row
    CSV.foreach(filename, :encoding => 'UTF-8') do |row|
        index = 0; i = 0; missing_col = 0; zero_distance = 1, zero_time = 1

        # Iterate throught the cols of the row that is read
        for x in row
            # Count the number of empty columns in row
            if x == nil
                # Incremeant i each time there is a col with no data
                # If i is less than 3, it will be added
                i = i + 1 

                # If a row is missing any of these cols, the row will not be appended to the dataset array
                if (index == $id || index == $severity || index == $zipcpde ||
                    index == $start_time || index == $end_time || 
                    index == $visibility || index == $weather_condition ||
                    index == $country)

                    missing_col = 1

                end
            end
            index = index + 1
        end

        # If distance is does not equal 0, zero_distance is false 
        # If zero_distance is 0, the row will be added
        if(row[$distance].to_f != 0.0)
            zero_distance = 0
        end
        
        # If the diference between End_time and Start_time is not 0
        # the row will be added
        if(row[$start_time] != row[$end_time])
            zero_time = 0
        end
            
    
        # If row had less than 3 empty cols and is not, missing any data from 
        # the cols specified, append to dataset array.
        if (i < 3 && missing_col == 0 && zero_distance == 0 && zero_time == 0)
            # Only consider the first 5 digits of the zip code
            row[$zipcpde] = row[$zipcpde][0,5]
            # Add a col containing the month of the accident to the row
            row.append(Date.parse(row[$start_time]).strftime("%B"))
            # Add a col containing the year of the accident to the row
            row.append(Date.parse(row[$start_time]).strftime("%Y"))
            # Finally append the row to the dataset array
            $dataset.append(row)
        end
        
    end

    puts("[#{Time.now - $starting}] Printing row count after data clean is finished")
    puts("[#{Time.now - $starting}] #{$dataset.length()}")
end


# In what month were there more accidents reported?
def worst_month()
    months = []
    index = 0
    for month in $dataset
        months.append($dataset[index][$month])
        index = index + 1
    end

    month = months.group_by(&:itself).values.max_by(&:size).first
    puts("[#{Time.now - $starting}] 1. In what month were there more accidents reported?")
    puts("[#{Time.now - $starting}] #{month} had the most accidents with a total of: #{months.count(month)}\n\n")
    months.clear

end

# 2. What is the state that had the most accidents in 2020?
def states_with_most_accidents_in_2020()
    accidents_2020 = []
    index = 0
    for row in $dataset
        if ($dataset[index][$year].to_i == 2020)
            accidents_2020.append($dataset[index][$state])
        end
        index = index + 1
    end

    state = accidents_2020.group_by(&:itself).values.max_by(&:size).first
    puts("[#{Time.now - $starting}] 2. What is the state that had the most accidents in 2020?")
    puts("[#{Time.now - $starting}] #{state} had the most accidents in 2020 with a total of: #{accidents_2020.count(state)}\n\n")
    accidents_2020.clear

end

# 3. What is the state that had the most accidents of severity 2 in 2021?
def most_accidents_of_severity_2_state()
    accidents_2021 = []
    index = 0
    for row in $dataset
        if ($dataset[index][$year].to_i == 2021 && $dataset[index][$severity].to_i == 2)
            accidents_2021.append($dataset[index][$state])
        end
        index = index + 1
    end

    state = accidents_2021.group_by(&:itself).values.max_by(&:size).first
    puts("[#{Time.now - $starting}] 3. What is the state that had the most accidents of severity 2 in 2021?")
    puts("[#{Time.now - $starting}] #{state} had the most accidents in 2021 with severity 2 with a total of: #{accidents_2021.count(state)}\n\n")
    accidents_2021.clear
end

# 4. What severity is the most common in Virginia?
def virginia_top_severity()
    virginia = []
    index = 0
    for row in $dataset
        if ($dataset[index][$state] == "VA")
            virginia.append($dataset[index][$severity])
        end
        index = index + 1
    end

    top_severity = virginia.group_by(&:itself).values.max_by(&:size).first
    puts("[#{Time.now - $starting}] 4. What severity is the most common in Virginia?")
    puts("[#{Time.now - $starting}] Severity #{top_severity} was most common in Virginia with a total of: #{virginia.count(top_severity)}\n\n")
    virginia.clear

end

# 5. What are the 5 cities that had the most accidents in 2019 in California?
def top_cities()
    california = []
    index = 0
    for row in $dataset
        if ($dataset[index][$state] == "CA" && $dataset[index][$year].to_i == 2019)
            california.append($dataset[index][$city])
        end
        index = index + 1
    end

    # top_cities is an array of hashes. Each element in array becomes an key and the value for each key is the number of occurnaces 
    top_cities = california.group_by(&:itself).transform_values(&:count)
    puts("[#{Time.now - $starting}] 5. What are the 5 cities that had the most accidents in 2019 in California?")
    puts("THIS STILL NEEDS WORKING: ONLY DISPLAYS THE 1st TOP CITY")
    puts(top_cities.max_by(&:last).first(5))
    # puts("#{top_city} was most common in CA with a total of: #{california.count(top_city)}")
    # california.clear
end

# 6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?
def avgerage_humidity_temp()
    temp = []
    humid = []
    index = 0
    for row in $dataset
        if ($dataset[index][$year].to_i == 2021 && $dataset[index][$severity].to_i == 4)
            temp.append($dataset[index][$temperature].to_f)
            humid.append($dataset[index][$humidity].to_f)
        end
        index = index + 1
    end

    puts("[#{Time.now - $starting}] 6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?")
    puts("[#{Time.now - $starting}] Average Temperature: #{temp.sum(0.0) / temp.size}")
    puts("[#{Time.now - $starting}] Average Humidity: #{humid.sum(0.0) / humid.size}\n\n")


end

def get_csv(csv_option)

    if (csv_option.chomp == "1")
        read_csv()

        return 1

    elsif (csv_option.chomp == "2")
        print("** Enter the name of the CSV you would like to read: ")
        filename = gets
        filename.chomp
        filename = filename.chomp + ".csv"

        # read in other CSV
        $starting = Time.now
        read_csv(filename)
        ending = Time.now
        elapsed = ending - $starting
        puts("#{elapsed}\n")
        
        return 1

    else
        puts("** You did not select a valid option")
        return 0
    end

end

def menu()
    puts("\nCMPS 3500 Project: \nPlease select an option\n")

    # Menu Options
    print("** Press '1' to read in a CSV: ")

    selection = gets
    selection.chomp

    if(selection.chomp == "1")

        puts("\nSelect an option:")
        puts("** 1: Read in US_Accidents.csv")
        puts("** 2: Enter the name of another CSV")
        print("** Select an option '1' or '2': ")
        csv_option = gets
        csv_option.chomp
        read_in = get_csv(csv_option)

    elsif
        abort("** Not a valid choice. \nExiting Program")

    end

    puts("\n** Option '1' to answer all 10 questions\n")
    puts("** Press anything else to quit\n")
    print("Select an option: ")
    selection = gets
    selection.chomp
    
    if (selection.chomp == "1" && read_in == 1)
        $starting = Time.now
        start = Time.now

        worst_month()
        states_with_most_accidents_in_2020()
        most_accidents_of_severity_2_state()
        virginia_top_severity()
        top_cities()
        avgerage_humidity_temp()

        ending = Time.now
        elapsed = ending - start
        puts(elapsed)
    
    # Selected 1 but did not read in a file
    elsif (selection.chomp == "1" && read_in == 0)
        abort("** You did not read in a file, you can't answer questions w/o reading in a file.\nExiting Program")

    else
        abort("** Exiting this program, Captain!")
    end
end

menu()
