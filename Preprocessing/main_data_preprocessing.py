# Preprocessing the main data

# Imports
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
import pyspark.sql.functions as fn

boro_dict = {
    'B' : 'BRONX',
    'S' : 'STATEN ISLAND',
    'K' : 'BROOKLYN',
    'M' : 'MANHATTAN',
    'Q' : 'QUEENS'
}

# Converting borough names to common used format
def convert_borough_name(row):
    row_dict = row.asDict()
    if(row_dict['BORO_NM'] in boro_dict):
        row_dict['BORO_NM'] = boro_dict[row_dict['BORO_NM']]
    return Row(**row_dict)

# Preprcessing the main data 
def preprocessing(arrests, complaints):

    # Dropping unnecessary columns
    arrests = arrests.drop('PD_CD','KY_CD','LAW_CODE', \
                            'LAW_CAT_CD','JURISDICTION_CODE','X_COORD_CD', \
                            'Y_COORD_CD')
    complaints = complaints.drop('CMPLNT_TO_DT','CMPLNT_TO_TM', \
                                'RPT_DT', 'KY_CD', 'PD_CD', 'LOC_OF_OCCUR_DESC', \
                                'JURIS_DESC','JURISDICTION_CODE','PARKS_NM','HADEVELOPT', \
                                'HOUSING_PSA','X_COORD_CD','Y_COORD_CD','TRANSIT_DISTRICT', \
                                'PATROL_BORO','STATION_NAME')

    # Renaming the arrest data columns
    arrests = arrests.withColumnRenamed("ARREST_BORO", "BORO_NM") \
                        .withColumnRenamed("ARREST_PRECINCT", "ADDR_PCT_CD") \
                        .withColumnRenamed("AGE_GROUP", "SUSP_AGE_GROUP") \
                        .withColumnRenamed("PERP_RACE", "SUSP_RACE") \
                        .withColumnRenamed("PERP_SEX", "SUSP_SEX")\
                        .withColumnRenamed("ARREST_KEY", "CMPLNT_NUM")
                    
    # Convert borough names for arrests data
    arrests = arrests.rdd.map(lambda row : convert_borough_name(row)).toDF()

    # Filtering age group
    arrests = arrests.filter(arrests.SUSP_AGE_GROUP.isin(['<18', '18-24' , '25-44', '45-64', '65+']))
    complaints = complaints.filter(complaints.SUSP_AGE_GROUP.isin(['<18', '18-24' , '25-44', '45-64', '65+', 'UNKNOWN']) | complaints.SUSP_AGE_GROUP.isNull())

    # Filtering sex
    complaints = complaints.filter(complaints.SUSP_SEX.isin(['M', 'F', 'U']) | complaints.SUSP_SEX.isNull())
    complaints = complaints.filter(complaints.VIC_SEX.isin(['M', 'F', 'U', 'D', 'E']) | complaints.VIC_SEX.isNull())


    # Coverting the columns to suitable datatype
    complaints = complaints.withColumn("ARREST_DATE", lit(None).cast(StringType())) \
    .withColumn("RECORD_TYPE", lit('C'))\
    .withColumn('Year', fn.year(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy'))) \
    .withColumn('Month', fn.month(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy'))) \
    .withColumn('Date', fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy')) 

    arrests = arrests.withColumn("CMPLNT_FR_DT", lit(None).cast(StringType())) \
    .withColumn("CMPLNT_FR_TM", lit(None).cast(StringType())) \
    .withColumn("OFNS_DESC", lit(None).cast(StringType())) \
    .withColumn("CRM_ATPT_CPTD_CD", lit(None).cast(StringType())) \
    .withColumn("LAW_CAT_CD", lit(None).cast(StringType())) \
    .withColumn("PREM_TYP_DESC", lit(None).cast(StringType())) \
    .withColumn("Lat_Lon", lit(None).cast(StringType())) \
    .withColumn("VIC_AGE_GROUP", lit(None).cast(StringType())) \
    .withColumn("VIC_RACE", lit(None).cast(StringType())) \
    .withColumn("VIC_SEX", lit(None).cast(StringType())) \
    .withColumn("RECORD_TYPE", lit('A'))\
    .withColumn('Year', fn.year(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy'))) \
    .withColumn('Month', fn.month(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy'))) \
    .withColumn('Date', fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy')) 

    # Combining datasets
    crime_data = arrests.union(complaints)

    return crime_data

if __name__ == "__main__":
    # Creating the spark sql session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Reading the data
    arrests_histroic = spark.read.json("hdfs://home/udit_gupta_1/input/Arrests_Historic.json")
    arrests_2019 = spark.read.json("hdfs://home/udit_gupta_1/input/Arrests_2019.json")
    complaints_histroic = spark.read.json("hdfs://home/udit_gupta_1/input/Complaints_historic.json")
    complaints_2019 = spark.read.json("hdfs://home/udit_gupta_1/input/Complaints_2019.json")

    # Merging to make arrests , complaints RDD
    arrests = arrests_histroic.union(arrests_2019)
    complaints = complaints_histroic.union(complaints_2019)

    # Preprocessing the data
    crime_data = preprocessing(arrests, complaints)

    # Writing the preprocessed data to HDFS
    crime_data.write.json("hdfs://home/udit_gupta_1/processed_data/")
