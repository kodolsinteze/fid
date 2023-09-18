import pandas as pd
import sqlite3
import requests
import json

def xml_to_json(url):
    """
    Convert XML data from the given URL to a JSON object.
    
    Parameters:
        url (str): The URL of the XML data to be converted.
    
    Returns:
        str: The JSON representation of the XML data.
            Returns None if the request fails.
    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Get the XML content from the response
        xml_data = response.text

        # Convert the Python dictionary to a JSON object
        json_data = json.loads(xml_data)
        
        return json_data

    else:
        print(f"Failed to retrieve data from '{url}' (status code: {response.status_code})")
        return None


def json_to_dataframe(data):
    """
    Converts a JSON object to a pandas DataFrame.

    Parameters:
        data (dict): The JSON object to convert.
        
    Returns:
        pandas.DataFrame: The resulting DataFrame.
    """
    # Create an empty dictionary to store the districts.
    districts = {}

    # Iterate over each district in the data.
    for district in data["value"]:
        
        # Iterate over each entry list element.
        districts[district['district_name']] = district['district_atvk']
    
    df = pd.DataFrame(list(districts.items()), columns=['district_name', 'district_atvk'])
    
    # Return the DataFrame.
    return df


# Read the CSV data from the uznemumu_registrs.csv file into a Uznemumu registrs dataframe
UR_data = pd.read_csv('uznemumu_registrs.csv', usecols=['regcode', 'name', 'address'])

#Pull data from web and make district_codes as a dataframe
url = "https://www.epakalpojumi.lv/odata/service/Districts"
data = xml_to_json(url)
district_codes=json_to_dataframe(data) 

#Adding address first and second word to each dataframes, so its more precise to merge, i.e., Ventspils novads: Ventspils = First_Word, novads = Second_Word
UR_data['First_Word'] = UR_data['address'].str.split().str[0] 
UR_data['Second_Word'] = UR_data['address'].str.split().str[1]
district_codes['First_Word'] = district_codes['district_name'].str.split().str[0]
district_codes['Second_Word'] = district_codes['district_name'].str.split().str[1]

#Extract to separate dataframe all companies which are in novadi
mask1 = UR_data["Second_Word"] == "nov." 
mask2 = district_codes["Second_Word"] == "novads" 
filtered_UR_data = UR_data[mask1]
filtered_district_codes = district_codes[mask2]
result_df_novadi = pd.merge(filtered_UR_data, filtered_district_codes, left_on="First_Word", right_on="First_Word", how="inner")

#Extract to separate dataframe all companies which are in cities
mask1_city = UR_data["Second_Word"].isna() 
mask2_city = district_codes["Second_Word"].isna()
filtered_UR_data_city = UR_data[mask1_city]
filtered_district_codes_city = district_codes[mask2_city]
result_df_city = pd.merge(filtered_UR_data_city, filtered_district_codes_city, left_on="First_Word", right_on="First_Word", how="inner")

#Merge both dataframes(novadi and cities)
filtered_df_combined = pd.concat([result_df_novadi, result_df_city])

#Merge filtered dataframes with the rest of the original dataframe
#This deals with edge cases, like rajoni, i.e., (Jekabpils rajons).
final_df = pd.merge(UR_data, filtered_df_combined, left_on="regcode", right_on="regcode", how="outer")
final_df.drop(["First_Word_x", "Second_Word", "name_y", "address_y", "First_Word_y", "Second_Word_x", "Second_Word_y"], axis=1, inplace=True)
final_df = final_df.rename(columns={"name_x": "name", "address_x": "address"})

# Specify the file path where you want to save the CSV file
csv_file_path = "processed_data.csv"

# Write the DataFrame to a CSV file
final_df.to_csv(csv_file_path, index=False, encoding='utf-8')

# Connect to SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect('database.sqlite')

# Write the data to a table named 'merged_table'
final_df.to_sql('merged_table', conn, if_exists='replace', index=False)

#Closing the db connection
conn.close()
