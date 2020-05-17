## Data Preprocessing

The raw data was carefully preprocessed to form data usable by EDA, regression.

### NYC crime data preprocessing - main_data_preprocessing.py
The NYC crime data is preprocessed and stored in HDFS for future usage. 
- Dropping unnecessary columns in datasets
- The borough names for arrests data is processed to readable form
- Filtering the data to remove rows with null values
- Converting the columns to suitable datatype
- Merging all datasets to get unified dataset 

### NYC demographics data preprocessing - secondary_data_preprocessing.py
The NYC demographics data is preprocessed using pandas (as its small file)
- Dropping unnecessary columns in datasets
- Converting the data frame into usable format
- Merging all datasets to get unified dataset 