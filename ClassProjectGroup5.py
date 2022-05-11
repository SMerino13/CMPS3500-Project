# COURSE: CMPS3500
# ASGN: CLASS Project
# DATE: 04/30/22
# Student 1: Steven Merino
# Student 2: Erica McKenna
# Student 3: Gregory Carag-Chiu
# Student 4: Bryan Ayapantecatl

import sys
import time
import threading
import re

# Check for and quit if environment is python2.
if (sys.version_info[0] == 2):
    print("\nCannot run script with python 2.x")
    print("Usage: python3 %s \n" % sys.argv[0])
    quit()
else:
    # import pandas if environment is python 3
    import pandas as pd 

# Global boolean to determine if a file has been read (FALSE BY DEFAULT)
read_data = False

# Function will load a CSV into a Dataframe using Panda's
def load_data(filename = "US_Accidents_data.csv"):
    global df, read_data, start_time

    print("Loading data set:")
    print("************************************")
    print("[",time.time() - start_time,"] Starting Script")
    print("[",time.time() - start_time,"] Loading",filename)

    try: 
        df = pd.read_csv(filename)
    except FileNotFoundError:
        # If file not found, return to menu
        print("UNABLE TO LOAD FILE!:",filename,"not found in directory")
        return False
    except:
        print("UNABLE TO LOAD FILE!:",filename)
        return False

    print("[",time.time() - start_time,"] Total Columns Read:",len(df.columns))
    print("[",time.time() - start_time,"] Total Rows Read:",len(df))

    return True

# Function perfors cleaning task specified in project
def process_data():
    global df, start_time
    print("Processing input data set:")
    print("**************************")
    print("[",time.time() - start_time,"] Performing Data Clean Up")

    # Eliminate rows will missing values in the following columns
    df = df.dropna(subset = ["ID", "Severity", "Zipcode", "Start_Time", "End_Time", "Visibility(mi)", "Weather_Condition", "Country"], thresh=8)

    # Eliminate all rows with empty values in 3 or more columns
    df = df.dropna(thresh = len(df.columns) - 2)

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

    print("[",time.time() - start_time,"] Total Rows Read after cleaning is:", len(df))

# THE FOLLOWING FUNCTIONS ANSWER THE 10 REQUIRED QUESTIONS
# ___________________________________________________________________________________

# 1. In what month were there more accidents reported?
def worst_month():
    global df, start_time

    month_list = [" ","January", "Feb", "March", "April", "May", "June", "July", 
        "August", "September", "October", "November", "December"]
    
    # Counts the occurrences of each month
    # value_counts()[:1] will get the top month once all occurrences have been counted
    top_month = pd.DatetimeIndex(df['Start_Time']).month.value_counts()[:1]

    num_accidents = top_month.tolist()
    month = top_month.index.tolist()
    month_index = month[0]

    print("[",time.time() - start_time,"] 1. In what month were there more accidents reported?")
    print("[",time.time() - start_time,"]",month_list[month_index],"had the most with",num_accidents[0],".\n")


# 2. What is the state that had the most accidents in 2020?
def worst_state_2020():
    global df, start_time

    # Year2022 is an array of boolean values where True signifies a row that occured in 2020
    year2020 = df["Start_Time"].dt.year == int(2020)
    # Only get rows where boolean values are True
    specifed_year = df[year2020]

    # Counts the occurrences of each state and gets the top once
    top_state = specifed_year["State"].value_counts()[:1]

    num_accidents = top_state.tolist()
    state = top_state.index.tolist()

    try:
        print("[",time.time() - start_time,"] 2. What is the state that had the most accidents in 2020?")
        print("[",time.time() - start_time,"]",state[0],"had the most with",num_accidents[0],"accidents.\n")
    except IndexError:
        print("[",time.time() - start_time,"] Not enough information to answer question\n")


# 3. What is the state that had the most accidents of severity 2 in 2021?
def most_accidents_of_severity_2_state():
    global df, start_time

    # Year2021 is an array of boolean values where True signifies a row that occured in 2021
    year2021 = df["Start_Time"].dt.year == int(2021)
    # Only get rows where boolean values are True
    specifed_year = df[year2021]

    # severe2 is an array of boolean values where True signifies a row with severity 2
    severe2 = specifed_year["Severity"] == int(2)
    # Only get rows where boolean values are True
    specifed_year = specifed_year[severe2]

    # Counts the occurrences of each state and gets the top once
    top_state = specifed_year["State"].value_counts()[:1]

    num_accidents = top_state.tolist()
    state = top_state.index.tolist()

    try:
        print("[",time.time() - start_time,"] 3. What is the state that had the most accidents of severity 2 in 2021?")
        print("[",time.time() - start_time,"]",state[0],"had the most with",num_accidents,"accidents.\n")
    except IndexError:
        print("[",time.time() - start_time,"] Not enough information to answer question\n")

        

# 4. What severity is the most common in Virginia?
def virginia_top_severity():
    global df, start_time

    virginia = df["State"] == "VA"
    specified_state = df[virginia]
    
    top_serverity = specified_state["Severity"].value_counts()[:1]

    num_accidents = top_serverity.tolist()
    serverity = top_serverity.index.tolist()

    try:
        print("[",time.time() - start_time,"] 4. What severity is the most common in Virginia?")
        print("[",time.time() - start_time,"] Severity",serverity[0],"was most common with",num_accidents[0],"accidents.\n")
    except IndexError:
        print("[",time.time() - start_time,"] Not enough information to answer question\n")
    

# 5. What are the 5 cities that had the most accidents in 2019 in California?
def top_cities():
    global df, start_time

    # Only get rows that correspond to 2019
    year2019 = df["Start_Time"].dt.year == int(2019)
    specifed_year = df[year2019]
    
    # Only get rows from 2019 that involve CA
    california = specifed_year["State"] == "CA"
    specified_state = specifed_year[california]

    top_cities = specified_state["City"].value_counts()[:5]

    print("[",time.time() - start_time,"] 5. What are the 5 cities that had the most accidents in 2019 in California?")

    if(len(top_cities)):
        print("[",time.time() - start_time,"] The following are the 5 cities with the most accidents in 2019 in CA\n"
            + str(top_cities.to_string()) + "\n")
    else:
        print("[",time.time() - start_time,"] There are no recorded of accidents for CA in 2019\n")


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

    print("[",time.time() - start_time,"] " 
        + "6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?")

    # If temp avg is nan there was not enough information to calculate avg
    if (pd.isna(avg_temp) == True):
        print("[",time.time() - start_time,"] No tempature recordings for this year and or severity")
    else:
        print("[",time.time() - start_time,"] The average temperature was: ",avg_temp)
    
    # If humidity avg is nan there was not enough information to calculate avg
    if (pd.isna(avg_hummid) == True):
        print("[",time.time() - start_time,"] No humidity recordings for this year and or severity\n")
    else:
        print("[",time.time() - start_time,"] The average humidity was: ",avg_hummid,"\n")


# 7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?
def common_conditions():
    global df, start_time

    # Grab the top 3 weather conditions 
    top_conditions = df["Weather_Condition"].value_counts()[:3]

    print("[",time.time() - start_time,"] " 
        + "7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?") 
    print("[",time.time() - start_time,"] Outputting up to 3 of the most common weather conditions:\n"
        + str(top_conditions.to_string()) + "\n")


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

    print("[",time.time() - start_time,"] " 
        + "8. What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?") 
    if (pd.isna(max_vis) == True):
        print("[",time.time() - start_time,"] Not enough information to answer question")
    else:
        print("[",time.time() - start_time,"]",max_vis,"miles\n")



# 9. How many accidents of each severity were recorded in Bakersfield?

def bakersfield_severity_count():
    global df, start_time

    # Only get rows that belong to Bako
    bakersfield = df["City"] == "Bakersfield"
    city = df[bakersfield]

    # Get the types of severity accidents and the total number of them
    severity = city["Severity"].value_counts().index.tolist()
    severity_count = city["Severity"].value_counts().tolist()


    print("[",time.time() - start_time,"] "
        + "9. How many accidents of each severity were recorded in Bakersfield?")
    if(len(severity)):
        for i in range (len(severity)):
            print("[",time.time() - start_time,"]",severity_count[i],"accidents recorded with severity",severity[i])
    else:
        print("[",time.time() - start_time,"] Not enough information to answer question")


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
    
    longest_acc = accident_len.max()
    longest_acc = longest_acc / pd.Timedelta(hours = 1)

    print("\n[" + str(time.time() - start_time) + "] " 
        + "10. What was the longest accident (in hours) recorded in Florida in the Spring"
        + " (March, April, and May) of 2022?") 

    # Condition to check if value is a real value, if not, no accident exist for year
    if (pd.isna(longest_acc) == True):
        print("[" + str(time.time() - start_time) + "] " 
            + "There are no recorded accidents in the year 2022 and or the slected months\n")
    else:
        print("[" + str(time.time() - start_time) + "] " 
            + str(longest_acc) + " hours\n")

# MOVE THIS LATER
# print("Total Running Time (In Minutes): " 
#     + str((time.time() - start_time) / 60) + "\n")

# This searches accidents by city, state. and zipcode
def search_location():
    global df
    is_state = False
    is_zip = False
    local_frame = df.copy()

    # Get user input for State, City, Zip
    # ________________________________________________________________________
    while(not is_state):
        # User enters state name
        input2 = input("Enter a state (abbreviated): ")

            # Ensures that the state they entered is only 2 letters
        state = re.sub(r'[^a-zA-Z]', "", input2)
        state = state.upper()
        if(len(state) == 2 or len(state) == 0):
            is_state = True

    # User enters city name
    input1 = input("Enter a city: ")

    # Ensures that the city the users entered only contains letters and spaces
    city = re.sub(r'[^a-zA-Z\s]', "", input1)
    city = city.lower()
    city = city.strip()
    city = city.title()
    
    while(not is_zip):
        # User enters state name
        input3 = input("Enter a (5) digit Zipcode [Letters will be ignored]: ")

        # Ensures that the zip entered is 5 numbers
        zipcode = re.sub(r'[^0-9]', "", input3)
        if(len(zipcode) == 5 and zipcode.isnumeric() or len(zipcode) == 0):
            is_zip = True
    # ________________________________________________________________________

    # Begin Filtering Data Based on Input
    # ________________________________________________________________________
    # Start Timer
    start_time = time.time()

    # If a City was entered, make a boolean array where True values are rows that contain that City
    if(city):
        city_mask = local_frame["City"] == city
        # IF there are no true values, that City does not exist in the Dataframe
        if(sum(city_mask) == 0):
            print(city,"is not a city listed in the file")
            return
        else:
            local_frame = local_frame[city_mask]

    # If a State was entered, make a boolean array where True values are rows that contain that State
    if(state):
        state_mask = local_frame["State"] == state
        # IF there are no true values, that state does not exist in the Dataframe
        if(sum(state_mask) == 0):
            print(state,"is not a state listed in the file")
            return
        else:
            local_frame = local_frame[state_mask]

    # If a Zipcode was entered, make a boolean array where True values are rows that contain that Zipcode
    if(zipcode):
        zip_mask = local_frame["Zipcode"] == zipcode
        # IF there are no true values, that Zipcode does not exist in the Dataframe
        if(sum(zip_mask) == 0):
            print(zipcode,"is not a zipcode listed in the file")
            return
        else:
            local_frame = local_frame[zip_mask]

    print("There were",len(local_frame),"accidents.")
    print("Time to perform search is:[",time.time() - start_time,"]\n")


def search_date():
    is_month = False
    is_year = False
    is_day = False
    user_input = False

    local_frame = df.copy()
    
    # Get user input for Month, Year, and Day
    # ________________________________________________________________________
    while (not is_year):
        year = input("\nEnter a [Year] using digits: ")
        
        if(year.isnumeric() and len(year) == 4):
            user_input = True
            is_year = True
        elif(len(year) == 0):
            is_year = True
        else:
           print("Invalid Input...\n")
    
    while (not is_month):
        month = input("Enter a [Month] using digits: ")
        
        if(month.isnumeric() and (int(month) > 0 and int(month) < 13)):
            user_input = True
            is_month = True
        elif(len(month) == 0):
            is_month = True
        else:
            print("Invalid Input...\n")

    while(not is_day):
        day = input("Enter a [Day] using digits: ")
        
        if(day.isnumeric() and (int(day) > 0 and int(day) < 32)):
            user_input = True
            is_day = True
        elif(len(day) == 0):
            is_day = True
        else:
            print("Invalid Input...\n")
    # ________________________________________________________________________
    # Filter dataframe using the inputted data
    # ________________________________________________________________________
    # Start Timer
    start_time = time.time()

    if(year):
        filter_year = local_frame["Start_Time"].dt.year == int(year)
        local_frame = local_frame[filter_year]

    if(month):
        filter_month = local_frame["Start_Time"].dt.month == int(month)
        local_frame = local_frame[filter_month]
    
    if(day):
        filter_day = local_frame["Start_Time"].dt.day == int(day)
        local_frame = local_frame[filter_day]
    # ________________________________________________________________________
    # Print Results
    # ________________________________________________________________________ 
    
    if(user_input):
        print("[",time.time() - start_time,"] There are:",len(local_frame),
            "accidents.\n")
    else:
        print("No Year, Month, or Day was inputed. Total Accidents",
            len(local_frame))

# search accidents by a range of temperature and by visibility
def search_temp():
    global df

    temperature = df.copy()

    # filter inputs here (wip)
    min_temp = int(input("Enter a Minimum Temperature (F): "))
    max_temp = int(input("Enter a Maximum Temperature (F): "))
    min_vis = int(input("Enter a Minimum Visibility (mi): "))
    max_vis = int(input("Enter a Maximum Visibility (mi): "))
    #

    if(min_temp):
        min_temp_mask = temperature["Temperature(F)"] >= min_temp
        if(sum(min_temp_mask) == 0):
            print(min_temp, "is a temp out of bounds not listed in the file")
            return
        else:
            temperature = temperature[min_temp_mask]

    if(max_temp):
        max_temp_mask = temperature["Temperature(F)"] <= max_temp
        if(sum(max_temp_mask) == 0):
            print(max_temp, "is a temp out of bounds not listed in the file")
            return
        else:
            temperature = temperature[max_temp_mask]

    if(min_vis):
        min_vis_mask = temperature["Temperature(F)"] >= min_vis
        if(sum(min_vis_mask) == 0):
            print(min_vis, "is a visibility out of bounds not listed in the file")
            return
        else:
            temperature = temperature[min_vis_mask]

    if(max_vis):
        max_vis_mask = temperature["Temperature(F)"] <= max_vis
        if(sum(max_vis_mask) == 0):
            print(max_vis, "is a visibility out of bounds not listed in the file")
            return
        else:
            temperature = temperature[max_vis_mask]

    print("[",time.time() - start_time,"] There are:", len(temperature),
        "accidents recorded.\n")
        
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

        # ASK USER FOR INPUT
        user_input = input("Select an option: ")

        if user_input == "1":
            # ASK FOR A FILENAME OR USE DEFAULT
            print("Enter the name or path of the file or press enter to read in US_Accidents_data")
            filename = input("[Do not append "'".csv"'"]: ")

            if(filename):
                filename = filename + ".csv"
                start_time = time.time()
                read_data = load_data(filename)
                load_time = time.time() - start_time
                print("Time to load is: [",load_time,"]\n")
            else:
                start_time = time.time()
                read_data = load_data()
                load_time = time.time() - start_time
                print("Time to load is: [",load_time,"]\n")
            
        # ONLY CLEAN DATA WHEN DATA HAS BEEN READ
        elif user_input == "2" and read_data == True:
            start_time = time.time()
            process_data()
            process_time = time.time() - start_time
            print("Time to process is:",process_time,)
            print("Total Runtime:[",load_time + process_time,"]\n")
        
        # ANSWER ALL 10 QUESTIONS
        elif user_input == "3" and read_data == True:
            print("\nAnswering Questions")
            print("*******************\n")

            # START TIMER
            start_time = time.time()

            # __________________________________________________
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
            # __________________________________________________

            # CALCULATE TIME
            question_time = time.time() - start_time

            print("Time to answer [",question_time,"]")
            print("Total runtime: [",load_time + process_time + question_time,"]\n")

        elif user_input == "4" and read_data == True:
            search_location()
        
        
        elif user_input == "5" and read_data == True: 
            search_date()

            '''
            if(len(input1) == 4):
                search_date(input1, input2, input3)
            else:
                print("ERROR")

            input2 = input("Enter a month: ")
            input3 = input("Enter a day: ")
            '''

        elif user_input == "6" and read_data == True:
            search_temp()

        # If user just presses enter w/o entering anything
        elif user_input == "7":
            quit = True

        else:
            print("\nINVALID: Pick a specified option or read in data first (option: '1')\n")

main_menu()