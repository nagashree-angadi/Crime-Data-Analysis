# Preprocessing demographics data
import pandas as pd
import numpy as np

# Demographics data available for following years
keep_column = ['2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']

# Merging the datasets
def merge_dataset(df1, df2):
    return pd.merge(df1, df2, how='inner', on=["Borough", "Year"])

# Preprocessing the data 
def preprocessing( location, type):
    data_frame = pd.read_csv(location).drop(columns=['short_name','long_name'])
    melted_data = pd.melt(data_frame, id_vars=['Borough'], value_vars = keep_column)
    melted_data = melted_data.rename(columns={'variable':'Year', 'value': type})
    return melted_data

if __name__ == "__main__":
    
    # Preprocessing the factors of demographic data
    poverty_rate = preprocessing("./Data/borough-povertyrate.csv", "proverty_rate")
    income_div = preprocessing("./Data/borough-incomediversityratio.csv", "income_diversity")
    racial_div = preprocessing("./Data/borough-racialdiversityindex.csv", "racial_diversity")
    unemployment = preprocessing("./Data/borough-unemploymentrate.csv", "unemployment_rate")
    population = preprocessing("./Data/borough-population.csv", "population")

    # Joining the datasets
    merge1 = merge_dataset(poverty_rate, income_div)
    merge2 = merge_dataset(racial_div, unemployment)
    merge3 = merge_dataset(merge2, population)
    demographics_df = merge_dataset(merge1, merge3)

    # Processing the borough names 
    demographics_df["Borough"] = demographics_df["Borough"].str.upper() 
    demographics_df["Demo_Key"] = demographics_df["Borough"]+demographics_df["Year"]

    # Writing the preprocessed data
    demographics_df.to_csv("./Data/borough_demographics.csv",  index=False )
