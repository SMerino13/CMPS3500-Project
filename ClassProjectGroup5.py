import pandas as pd 
import time
import threading

read_data = False

def data_proccessing():
    global df, read_data
    # Data-cleaning 
    ##############################################################################

    print("Loading and cleaning input data set:")
    print("************************************")
    print("[" + str(time.time() - start_time) + "] Starting Script")

    print("[" + str(time.time() - start_time) + "] Loading US_Accidents_data.csv")

    try: 
        df = pd.read_csv("US_Accidents_data.csv", index_col="Unnamed: 0")
        # df = pd.read_csv("OldDroptest.csv", index_col="Unnamed: 0")
    except FileNotFoundError:
        print("File: US_Accidents_data.csv not found")
        exit()

    print("[" + str(time.time() - start_time) + "] Performing Data Clean Up")

    df = df.dropna(subset = ["ID", "Severity", "Zipcode", "Start_Time", "End_Time", "Visibility(mi)", "Weather_Condition", "Country"], thresh=8)

    # Eliminate all rows with empty values in 3 or more columns
    df = df.dropna(thresh=19)

    # Eliminate all rows with distance equal to zero
    df = df[df["Distance(mi)"] != 0]

    # Only consider in your analysis the first 5 digits of the zip code
    df["Zipcode"] = df["Zipcode"].str[:5]

    # Convert string dates to pandas Timestamps
    df['Start_Time'] = pd.to_datetime(df['Start_Time'])
    df['End_Time'] = pd.to_datetime(df['End_Time'])

    # All accicent that lasted no time 
    # (The diference between End_time and Start_time is zero)
    # If accicent Start_Time and End_Time are the same that equates to a diff of 0
    df = df[df["Start_Time"] != df["End_Time"]]

    print("[" + str(time.time() - start_time) + "] Printing row count after data clean is finished")
    print("[" + str(time.time() - start_time) + "] " + str(len(df)))
    read_data = True

    ##############################################################################


# 1. In what month were there more accidents reported?
def da_month_with_most_accidents():
    global df, start_time, results
    month_list = ["January", "Feb", "March", "April", "May", "June", "July", 
        "August", "September", "October", "November", "December"]
    top_month = pd.DatetimeIndex(df['Start_Time']).month.value_counts()[:1]
    num_accidents = top_month.tolist()
    month = top_month.index.tolist()
    month_index = month[0] - 1

    print("\n[" + str(time.time() - start_time) + "] " 
    + "1. In what month were there more accidents reported?")

    print("[" + str(time.time() - start_time) + "] " + "In " 
        + month_list[month_index] 
        + " there were more accidents reported with a total of: " 
        + str(num_accidents[0]) + "\n")

# 2. What is the state that had the most accidents in 2020?
def states_with_most_accidents_in_2022():
    global df, start_time, results
    # Get rows from 2020 only
    year2020 = df["Start_Time"].dt.year == int(2020)
    specifed_year = df[year2020]

    # Count the total number of times states appear in the column
    top_state = specifed_year["State"].value_counts()[:1]
    # Grab the number of times it appeared
    num_accidents = top_state.tolist()
    # The index of the dataframe top_state is the state name
    state = top_state.index.tolist()

    print("[" + str(time.time() - start_time) + "] " 
        + "2. What is the state that had the most accidents in 2020?") 

    print("[" + str(time.time() - start_time) + "] " 
        + state[0] + " had the most accidents in 2020 with a total of :" 
        + str(num_accidents[0]) + "\n")


# 3. What is the state that had the most accidents of severity 2 in 2021?
def most_accidents_of_severity_2_state():
    global df, start_time, results

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

    print("[" + str(time.time() - start_time) + "] " 
        + "3. What is the state that had the most accidents of severity 2 in 2021?") 
        
    print("[" + str(time.time() - start_time) + "] " 
        + state[0] + " had the most accidents of severity 2 in 2021 with a total of: "
        + str(num_accidents[0]) + "\n")

    # print(specifed_year.head(15))
    # print(index.to_list())

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

    print("[" + str(time.time() - start_time) + "] " 
        + "4. What severity is the most common in Virginia?") 
    print("[" + str(time.time() - start_time) + "] " 
        + "Severity: " + str(serverity[0]) + " was most common in Virginia with a total of: " 
        + str(num_accidents[0]) + " accidents\n")
    

# 5. What are the 5 cities that had the most accidents in 2019 in California?
def top_cities():
    global df, start_time, results

    # Only get rows that correspond to 2019
    year2019 = df["Start_Time"].dt.year == int(2019)
    specifed_year = df[year2019]
    
    # Only get rows from 2019 that invlove CA
    california = specifed_year["State"] == "CA"
    specified_state = specifed_year[california]

    top_cities = specified_state["City"].value_counts()[:5]

    print("[" + str(time.time() - start_time) + "] " 
        + "5. What are the 5 cities that had the most accidents in 2019 in California?") 
    print("[" + str(time.time() - start_time) + "] " 
        + "The following are the 5 cities with the most accidents in 2019 in CA\n" 
        + top_cities.to_string() + "\n")


# 6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?

def avgerage_humidity_temp():
    global df, start_time, results

    # Only get rows that correspond to 2021
    year2021 = df["Start_Time"].dt.year == int(2021)
    specifed_year = df[year2021]

    # Only get rows that have a severity of 4
    severity_4 = specifed_year["Severity"] == int(4)
    specifed_severity = specifed_year[severity_4]

    # 
    avg_temp = specifed_severity["Temperature(F)"].mean()
    avg_hummid = specifed_severity["Humidity(%)"].mean()

    print("[" + str(time.time() - start_time) + "] " 
        + "6. What was the average humidity and average temperature of all accidents of severity 4 that occurred in 2021?") 
    print("[" + str(time.time() - start_time) + "] " 
        + "Average temperature: " + str(avg_temp) + " Average humidity: " 
        + str(avg_hummid) + "\n")


# 7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?

def common_conditions():
    global df, start_time, results

    top_conditions = df["Weather_Condition"].value_counts()[:3]

    print("[" + str(time.time() - start_time) + "] " 
        + "7. What are the 3 most common weather conditions (weather_conditions) when accidents occurred?") 
    print("[" + str(time.time() - start_time) + "] " 
        + "The 3 most common are the following:\n" 
        + top_conditions.to_string() + "\n")


# 8. What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?

def max_visibility():
    global df, start_time, results

    # Only get rows that correspond to New Hampshire
    new_hampshire = df["State"] == "NH"
    specfied_state = df[new_hampshire]

    # Only ger rows that have a Severity of 2
    severity = specfied_state["Severity"] == int(2)
    severity_2 = specfied_state[severity]

    # Get the max Visibility from the new constructed df
    max_vis = severity_2["Visibility(mi)"].max()

    print("[" + str(time.time() - start_time) + "] " 
        + "8. What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?") 
    print("[" + str(time.time() - start_time) + "] " 
        + str(max_vis) + "\n")



# 9. How many accidents of each severity were recorded in Bakersfield?

def bakersfield_severity_count():
    global df, start_time, results

    # Only get rows that belong to Bako
    bakersfield = df["City"] == "Bakersfield"
    city = df[bakersfield]

    # Get the types of severity accidents and the total number of them
    severity = city["Severity"].value_counts().index.tolist()
    severity_count = city["Severity"].value_counts().tolist()


    print("[" + str(time.time() - start_time) + "] " + 
        "9. How many accidents of each severity were recorded in Bakersfield?")
    for i in range (len(severity)):
        print("[" + str(time.time() - start_time) + "] " 
            + str(severity_count[i]) + " accidents recorded " 
            + "with severity " + str(severity[i]))


# 10. What was the longest accident (in hours) recorded in Florida in the Spring (March, April, and May) of 2022?

def longest_accident():
    global df, start_time, results
    
    # Only get rows that correspond to 2022
    year2022 = df["Start_Time"].dt.year == int(2022)
    specifed_year = df[year2022]

    # Only get rows that correspond to Florida
    florida = specifed_year["State"] == "FL"
    state = specifed_year[florida]

    # Only get rows that correspond to March, April, and May
    specified_months = state.loc[
        (state['Start_Time'].dt.month == int(3)) | 
        (state['Start_Time'].dt.month == int(4)) | 
        (state['Start_Time'].dt.month == int(5))]

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
        print("[" + str(time.time() - start_time) + "] " 
            + "10. What was the longest accident (in hours) recorded in Florida in the Spring (March, April, and May) of 2022?") 
        print("[" + str(time.time() - start_time) + "] " 
            + str(longest_acc) + " hours\n")

# MOVE THIS LATER
# print("Total Running Time (In Minutes): " 
#     + str((time.time() - start_time) / 60) + "\n")

def main_menu():
    global start_time, read_data, results

    print("\nCMPS 3500 Project: \nPlease select an option\n")

    # Menu options
    print("Press '1' to read in US_Accidents_data.csv into dataframe\n")
    print("Press '2' to answer all 10 questions\n")
    print("Press '3' to quit\n")
    # user input
    user_input = input("Select an option '1' or '2' or '3': ")

    # If utilizing threading store print results in list to print them out in order later
    # results = []
    # for i in range (10):
    #     results.append([])

    if user_input == "1":
        read_data = True
        start_time = time.time()
        data_proccessing()
        main_menu()
    
    elif user_input == "2" and read_data == True:
        print("\nAnswering Questions\n*******************")
        # Start timer
        start_time = time.time()

        da_month_with_most_accidents()
        states_with_most_accidents_in_2022()
        most_accidents_of_severity_2_state()
        virginia_top_severity()
        top_cities()
        avgerage_humidity_temp()
        common_conditions()
        max_visibility()
        bakersfield_severity_count()
        longest_accident()

        # Threading for questions

        # t1 = threading.Thread(target= da_month_with_most_accidents)
        # t2 = threading.Thread(target= states_with_most_accidents_in_2022)
        # t3 = threading.Thread(target= most_accidents_of_severity_2_state)
        # t4 = threading.Thread(target= virginia_top_severity)
        # t5 = threading.Thread(target= top_cities)
        # t6 = threading.Thread(target= avgerage_humidity_temp)
        # t7 = threading.Thread(target= common_conditions)
        # t8 = threading.Thread(target= max_visibility)
        # t9 = threading.Thread(target= bakersfield_severity_count)
        # t10 = threading.Thread(target=longest_accident)

        # t1.start()
        # t2.start()
        # t3.start()
        # t4.start()
        # t5.start()
        # t6.start()
        # t7.start()
        # t8.start()
        # t9.start()
        # t10.start()

        # t1.join()
        # t2.join()
        # t3.join()
        # t4.join()
        # t5.join()
        # t6.join()
        # t7.join()
        # t8.join()
        # t9.join()
        # t1.join()

        # for i in range (10):
        #     print(results[i])

        print("Total Running Time: "  + str(time.time() - start_time) + "\n")
        main_menu()

    elif user_input == "3":
        exit()
    
    # If user just presses enter w/o entering anything
    elif user_input == "":
        print("You did not pick an option\n")
        main_menu()

    else:
        print("\nINVALID: Pick a specified option or read in data first (option: '1')\n")
        main_menu()


main_menu()

