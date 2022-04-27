import sys
import time
import threading
import re

# Check for and quit if environment is python2.
if (sys.version_info[0] == 2):
    print("\nCannot run script with python 2.x")
    print ("Usage: python3 %s \n" % sys.argv[0])
    quit()

# import pandas if environment is python 3
import pandas as pd 

read_data = False

def load_data(filename = "US_Accidents_data.csv"):
    global df, read_data, start_time
    # Data-cleaning 
    ##############################################################################

    print("Loading and cleaning input data set:")
    print("************************************")
    print(f"[{time.time() - start_time}] Starting Script")
    print(f"[{time.time() - start_time}] Loading {filename}")

    try: 
        df = pd.read_csv(filename)
        # df = pd.read_csv("US_Accidents_data.csv", index_col="Unnamed: 0")
    except FileNotFoundError:
        print("File: US_Accidents_data.csv not found")
        exit()

    print(f"[{time.time() - start_time}] Total Columns Read: {len(df.columns)}")
    print(f"[{time.time() - start_time}] Total Rows Read: {len(df)}")

    read_data = True

def process_data():
    global df, start_time
    print("Processing input data set:")
    print("**************************")
    print(f"[{time.time() - start_time}] Performing Data Clean Up")

    df = df.dropna(subset = ["ID", "Severity", "Zipcode", "Start_Time", "End_Time", "Visibility(mi)", "Weather_Condition", "Country"], thresh=8)

    # Eliminate all rows with empty values in 3 or more columns
    df = df.dropna(thresh = len(df.columns) - 2)

    # If using the unnamed column in the CSV as index, thresh = 19
    # df = df.dropna(thresh=19)

    # Eliminate all rows with distance equal to zero
    df = df[df["Distance(mi)"] != 0]

    # Only consider in your analysis the first 5 digits of the zip code
    df["Zipcode"] = df["Zipcode"].str[:5]

    # Convert string dates to pandas Timestamps
    df['Start_Time'] = pd.to_datetime(df['Start_Time'])
    df['End_Time'] = pd.to_datetime(df['End_Time'])

    # All accicent that lasted no time 
    # If accicent Start_Time and End_Time are the same that equates to a diff of 0
    # Only kepp rows with different Start_Times and End_Times
    df = df[df["Start_Time"] != df["End_Time"]]

    print(f"[{time.time() - start_time}] Total Rows Read after cleaning is: {len(df)}")

##############################################################################


# 1. In what month were there more accidents reported?
def worst_month():
    global df, start_time
    month_list = ["January", "Feb", "March", "April", "May", "June", "July", 
        "August", "September", "October", "November", "December"]
    top_month = pd.DatetimeIndex(df['Start_Time']).month.value_counts()[:1]
    num_accidents = top_month.tolist()
    month = top_month.index.tolist()
    month_index = month[0] - 1

    print(f"[{time.time() - start_time}] 1. In what month were there more accidents reported?")
    print(f"[{time.time() - start_time}] {month_list[month_index]} had the most with {num_accidents[0]}.\n")


# 2. What is the state that had the most accidents in 2020?
def worst_state_2020():
    global df, start_time
    # Get rows from 2020 only
    year2020 = df["Start_Time"].dt.year == int(2020)
    specifed_year = df[year2020]

    # Count the total number of times states appear in the column
    top_state = specifed_year["State"].value_counts()[:1]
    # Grab the number of times it appeared
    num_accidents = top_state.tolist()
    # The index of the dataframe top_state is the state name
    state = top_state.index.tolist()

    print(f"[{time.time() - start_time}] 2. What is the state that had the most accidents in 2020?")
    print(f"[{time.time() - start_time}] {state[0]} had the most with {num_accidents[0]} accidents.\n")


# 3. What is the state that had the most accidents of severity 2 in 2021?
def most_accidents_of_severity_2_state():
    global df, start_time
    # Get rows with specified year
    year2021 = df["Start_Time"].dt.year == int(2021)
    specifed_year = df[year2021]

    # Now get rows that only have severity 2
    index = specifed_year["Severity"] == int(2)
    specifed_year = specifed_year[index]

    # Get the top state from new dataframe
    top_state = specifed_year["State"].value_counts()[:1]
    # Grab the number of times it appeared
    num_accidents = top_state.tolist()
    # The index of the dataframe top_state is the state name
    state = top_state.index.tolist()

    print(f"[{time.time() - start_time}] 3. What is the state that had the most accidents of severity 2 in 2021?")
    print(f"[{time.time() - start_time}] {state[0]} had the most with {num_accidents} accidents.\n")
        

# 4. What severity is the most common in Virginia?
def virginia_top_severity():
    global df, start_time

    # New dataframe with rows that contain only the state VA
    virginia = df["State"] == "VA"
    specified_state = df[virginia]
    
    top_serverity = specified_state["Severity"].value_counts()[:1]
    # Grab the number of times it appeared
    num_accidents = top_serverity.tolist()
    # The index of the dataframe top_state is the state name
    serverity = top_serverity.index.tolist()

    print(f"[{time.time() - start_time}] 4. What severity is the most common in Virginia?")
    print(f"[{time.time() - start_time}] Severity {serverity[0]} was most common with {num_accidents[0]} accidents.\n")
    

# 5. What are the 5 cities that had the most accidents in 2019 in California?
def top_cities():
    global df, start_time

    # Only get rows that correspond to 2019
    year2019 = df["Start_Time"].dt.year == int(2019)
    specifed_year = df[year2019]
    
    # Only get rows from 2019 that invlove CA
    california = specifed_year["State"] == "CA"
    specified_state = specifed_year[california]

    top_cities = specified_state["City"].value_counts()[:5]
    
    print(f"[{time.time() - start_time}] 5. What are the 5 cities that had the most accidents in 2019 in California?")
    print(f"[{time.time() - start_time}] The following are the 5 cities with the most accidents in 2019 in CA"
        + f"\n{top_cities.to_string()}\n")


# 6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?
def avgerage_humidity_temp():
    global df, start_time

    # Only get rows that correspond to 2021
    year2021 = df["Start_Time"].dt.year == int(2021)
    specifed_year = df[year2021]

    # Only get rows that have a severity of 4
    severity_4 = specifed_year["Severity"] == int(4)
    specifed_severity = specifed_year[severity_4]

    # Caulcaute the average temp and humidity from the remaining rows
    avg_temp = specifed_severity["Temperature(F)"].mean()
    avg_hummid = specifed_severity["Humidity(%)"].mean()

    print(f"[{time.time() - start_time}] " 
        + "6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?")
    # If temp avg is nan there was not enough information to calculate avg
    if (pd.isna(avg_temp) == True):
        print(f"[{time.time() - start_time}] No tempature recordings for this year and or severity")
    
    # If humidity avg is nan there was not enough information to calculate avg
    if (pd.isna(avg_hummid) == True):
        print(f"[{time.time() - start_time}] No humidity recordings for this year and or severity")

    # If both are values, print the results
    if ((pd.isna(avg_temp) == False) and (pd.isna(avg_hummid) == False)):
        print(f"[{time.time() - start_time}] Average Temp: {avg_temp} Average Humidity {avg_hummid}.\n")


# 7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?
def common_conditions():
    global df, start_time

    # Grab the top 3 weather conditions 
    top_conditions = df["Weather_Condition"].value_counts()[:3]

    print(f"[{time.time() - start_time}] " 
        + "7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?") 
    print(f"[{time.time() - start_time}]" 
        + "These were the 3 most common along with the corresponding number of accidents:\n" 
        + f"{top_conditions.to_string()}\n")


# 8. What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?
def max_visibility():
    global df, start_time

    # Only get rows that correspond to New Hampshire
    new_hampshire = df["State"] == "NH"
    specfied_state = df[new_hampshire]

    # Only ger rows that have a Severity of 2
    severity = specfied_state["Severity"] == int(2)
    severity_2 = specfied_state[severity]

    # Get the max Visibility from the new constructed df
    max_vis = severity_2["Visibility(mi)"].max()

    print(f"[{time.time() - start_time}] " 
        + "8. What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?") 
    print(f"[{time.time() - start_time}] {max_vis} miles\n")



# 9. How many accidents of each severity were recorded in Bakersfield?

def bakersfield_severity_count():
    global df, start_time

    # Only get rows that belong to Bako
    bakersfield = df["City"] == "Bakersfield"
    city = df[bakersfield]

    # Get the types of severity accidents and the total number of them
    severity = city["Severity"].value_counts().index.tolist()
    severity_count = city["Severity"].value_counts().tolist()


    print(f"[{time.time() - start_time}] "
        + "9. How many accidents of each severity were recorded in Bakersfield?")
    for i in range (len(severity)):
        print(f"[{time.time() - start_time}] "
            + f"{severity_count[i]} accidents recorded " 
            + f"with severity {severity[i]}")


# 10. What was the longest accident (in hours) recorded in Florida in the Spring (March, April, and May) of 2022?

def longest_accident():
    global df, start_time
    
    # Only get rows that correspond to 2022
    year2022 = df["Start_Time"].dt.year == int(2020)
    specifed_year = df[year2022]

    # Only get rows that correspond to Florida
    florida = specifed_year["State"] == "FL"
    state = specifed_year[florida]

    # Only get rows that correspond to March, April, and May
    specified_months = state.loc[
        (state['Start_Time'].dt.month == int(3)) | 
        (state['Start_Time'].dt.month == int(4)) | 
        (state['Start_Time'].dt.month == int(5)) ]

    # Find the longest accident
    # The longest accident will have the greatest differnce between Start_Time
    # and End_time

    accident_len = specified_months["End_Time"] - specified_months["Start_Time"]
    
    # longest_acc = accident_len.idxmax()
    longest_acc = accident_len.max()
    longest_acc = longest_acc / pd.Timedelta(hours = 1)
    # print(accident_len)

    # Condition to check if value is a real value, if not, no accident exist for year
    if (pd.isna(longest_acc) == True):
        print("\n[" + str(time.time() - start_time) + "] " 
            + "10. What was the longest accident (in hours) recorded in Florida in the Spring (March, April, and May) of 2022?")
        print("[" + str(time.time() - start_time) + "] " 
            + "There are no recorded accidents in the year 2022\n")

    else:
        print("\n[" + str(time.time() - start_time) + "] " 
            + "10. What was the longest accident (in hours) recorded in Florida in the Spring (March, April, and May) of 2022?") 
        print("[" + str(time.time() - start_time) + "] " 
            + str(longest_acc) + " hours\n")

# MOVE THIS LATER
# print("Total Running Time (In Minutes): " 
#     + str((time.time() - start_time) / 60) + "\n")

# This searches accidents by city, state. and zipcode
def search_location(input_city, input_state, input_zipcode):
    global df

    print("called")
    if(input_city):
        # Ensures that the city the users entered only contains letters and spaces
        city = re.sub(r'[^a-zA-Z\s]', "", input_city)
        city = city.lower()
        city = city.strip()
        city = city.title()

        city_df = df[df["City"] == city]
        if(city_df.empty):
            print(f"'{city}' is not a city listed in the file")
            return
    else:
        pass

##############################################################################

def main_menu():
    global start_time, read_data

    quit = False

    while(not quit):

        print("\nCMPS 3500 Project: \nPlease select an option\n")

        # Menu options
        print("(1) Load data")
        print("(2) Process data")
        print("(3) Print Answers")
        print("(4) Search Accidents (Use City, State, and Zip Code)")
        print("(5) Search Accidents (Year, Month and Day)")
        print("(6) Search Accidents (Temperature Range and Visibility Range)")
        print("(7) Quit\n")

        # user input
        user_input = input("Select an option: ")

        if user_input == "1":
            read_data = True
            start_time = time.time()
            load_data()
            load_time = time.time() - start_time
            print(f"TIme to load is: [{load_time}]\n")
            

        elif user_input == "2" and read_data == True:
            start_time = time.time()
            process_data()
            process_time = time.time() - start_time
            print(f"Time to process is: {process_time}\n")
            print(f"Total Runtime: {load_time + process_time}")
        
        elif user_input == "3" and read_data == True:
            print("\nAnswering Questions")
            print("*******************\n")

            # Start timer
            start_time = time.time()

            worst_month()
            worst_state_2020()
            most_accidents_of_severity_2_state()
            virginia_top_severity()
            top_cities()
            avgerage_humidity_temp()
            common_conditions()
            max_visibility()
            bakersfield_severity_count()
            longest_accident()

            question_time = time.time() - start_time
            print(f"Time to answer: {question_time}")
            print(f"Total runtime: {load_time + process_time + question_time}")

        elif user_input == "4" and read_data == True:
            # User enters city name
            input1 = input("Enter a city: ")

            is_state = False
            while(not is_state):
                input2 = input("Enter a state (abbreviated): ")
                state = re.sub(r'[^a-zA-Z]', "", input2)
                state = state.upper()
                if(len(state) == 2 or len(state) == 0):
                    is_state = True
            
            is_zip = False
            while(not is_zip):
                input3 = input("Enter a (5) digit Zipcode: ")
                zipcode = re.sub(r'[^0-9]', "", input3)
                if(len(zipcode) == 5 and zipcode.isnumeric() or len(zipcode) == 0):
                    is_zip = True

            # search_location(input1, state, zipcode)
        
        # If user just presses enter w/o entering anything
        elif user_input == "9":
            quit = True

        else:
            print("\nINVALID: Pick a specified option or read in data first (option: '1')\n")

main_menu()

