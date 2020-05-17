
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
spark = SparkSession.builder.master("local[*]").config("spark.jars", "/content/drive/My\ Drive/geospark-sql_2.3-1.3.1.jar,/content/drive/My\ Drive/geospark-1.3.1.jar").getOrCreate()




final_df = spark.read.format("csv").option("header", "true").load("your_data").limit(10000)




from pyspark.sql.types import IntegerType, StructField, StructType
from geospark.sql.types import GeometryType
import geopandas as gpd
from pyspark.sql import SparkSession
from geospark.register import GeoSparkRegistrator


points = gpd.read_file("/content/drive/My Drive/sde-columbia-census_2000_032807211977000-shapefile.zip (Unzipped Files)/columbia_census_2000_032807211977000.shp")




points.dtypes




points_geom = spark.createDataFrame(
    points[["bor_subb","name","geometry"]].astype(str)
)




points_geom.show(5,False)




points_geom.dtypes




import pyspark.sql.functions as f
from pyspark.sql.functions import concat, col, lit
complaints_df = final_df.filter(final_df.RECORD_TYPE == 'C')
complaints_df = complaints_df.filter(complaints_df.Lat_Lon.isNotNull())
complaints_df = complaints_df.withColumn("LatLon",f.concat(lit("POINT("), f.col("Longitude"),lit(" "),f.col("Latitude"), lit(")")))
complaints_df.take(2)




complaints_df.createOrReplaceTempView("county")
complaints_df.take(2)




points_geom.createOrReplaceTempView("pts")
points_geom.dtypes




from pyspark.sql import SparkSession
from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator
upload_jars()
GeoSparkRegistrator.registerAll(spark)
counties_geom = spark.sql(
    "SELECT *, st_geomFromWKT(LatLon) as point from county"
)
pts = spark.sql(
    "SELECT *,st_geomFromWKT(geometry) as shape from pts "
)
counties_geom.show(5, False)




pts.dtypes




counties_geom.createOrReplaceTempView("counties")
pts.createOrReplaceTempView("pois")




output = spark.sql(
    """
        SELECT c.CMPLNT_FR_DT,c.CMPLNT_FR_TM,c.ADDR_PCT_CD,c.Latitude,c.Longitude, p.bor_subb,p.name
        FROM pois AS p, counties AS c
        WHERE ST_Intersects(p.shape, c.point)
    """
)




merged_df = spark.read.format("csv").option("header", "true").load('/content/drive/My Drive/sub.csv')
merged_df.count()




from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import year
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
import pyspark.sql.functions as fn
from pyspark.sql.functions import countDistinct
from scipy import stats
import pandas as pd
import numpy as np
import six




# Reading the data
arrests_histroic = spark.read.json("hdfs://ahalya/raw_data/Arrests_2019.json")
arrests_2019 = spark.read.json("hdfs://ahalya/raw_data/Arrests_2019.json")
complaints_histroic = spark.read.json("hdfs://ahalya/raw_data/Complaints_historic.json")
complaints_2019 = spark.read.json("hdfs://ahalya/raw_data/Complaints_2019.json")




# Preprocessing Utility Functions

boro_dict = {
    'B' : 'BRONX',
    'S' : 'STATEN ISLAND',
    'K' : 'BROOKLYN',
    'M' : 'MANHATTAN',
    'Q' : 'QUEENS'
}

def convert_borough_name(row):
    row_dict = row.asDict()
    if(row_dict['BORO_NM'] in boro_dict):
        row_dict['BORO_NM'] = boro_dict[row_dict['BORO_NM']]
    return Row(**row_dict)




# Preprocessing

# Merging to make arrests , complaints RDD
arrests = arrests_histroic.union(arrests_2019)
complaints = complaints_histroic.union(complaints_2019)

# Dropping unnecessary tables
arrests = arrests.drop('PD_CD','KY_CD','LAW_CODE',                         'LAW_CAT_CD','JURISDICTION_CODE','X_COORD_CD',                         'Y_COORD_CD')
complaints = complaints.drop('CMPLNT_TO_DT','CMPLNT_TO_TM',                                'RPT_DT', 'KY_CD', 'PD_CD', 'LOC_OF_OCCUR_DESC',                                'JURIS_DESC','JURISDICTION_CODE','PARKS_NM','HADEVELOPT',                                'HOUSING_PSA','X_COORD_CD','Y_COORD_CD','TRANSIT_DISTRICT',                                'PATROL_BORO','STATION_NAME')

# Renaming the arrest columns
arrests = arrests.withColumnRenamed("ARREST_BORO", "BORO_NM")                     .withColumnRenamed("ARREST_PRECINCT", "ADDR_PCT_CD")                     .withColumnRenamed("AGE_GROUP", "SUSP_AGE_GROUP")                     .withColumnRenamed("PERP_RACE", "SUSP_RACE")                     .withColumnRenamed("PERP_SEX", "SUSP_SEX")                    .withColumnRenamed("ARREST_KEY", "CMPLNT_NUM")
                
# Convert borough names for arrests
arrests = arrests.rdd.map(lambda row : convert_borough_name(row)).toDF()

# Filtering age group
arrests = arrests.filter(arrests.SUSP_AGE_GROUP.isin(['<18', '18-24' , '25-44', '45-64', '65+']))
complaints = complaints.filter(complaints.SUSP_AGE_GROUP.isin(['<18', '18-24' , '25-44', '45-64', '65+', 'UNKNOWN']) | complaints.SUSP_AGE_GROUP.isNull())

# Filtering sex
complaints = complaints.filter(complaints.SUSP_SEX.isin(['M', 'F', 'U']) | complaints.SUSP_SEX.isNull())
complaints = complaints.filter(complaints.VIC_SEX.isin(['M', 'F', 'U', 'D', 'E']) | complaints.VIC_SEX.isNull())


# Coverting the columns to suitable datatype
complaints = complaints.withColumn("ARREST_DATE", lit(None).cast(StringType())) .withColumn("RECORD_TYPE", lit('C')).withColumn('Year', fn.year(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy')))

arrests = arrests.withColumn("CMPLNT_FR_DT", lit(None).cast(StringType())) .withColumn("CMPLNT_FR_TM", lit(None).cast(StringType())) .withColumn("OFNS_DESC", lit(None).cast(StringType())) .withColumn("CRM_ATPT_CPTD_CD", lit(None).cast(StringType())) .withColumn("LAW_CAT_CD", lit(None).cast(StringType())) .withColumn("PREM_TYP_DESC", lit(None).cast(StringType())) .withColumn("Lat_Lon", lit(None).cast(StringType())) .withColumn("VIC_AGE_GROUP", lit(None).cast(StringType())) .withColumn("VIC_RACE", lit(None).cast(StringType())) .withColumn("VIC_SEX", lit(None).cast(StringType())) .withColumn("RECORD_TYPE", lit('A')).withColumn('Year', fn.year(fn.to_timestamp('CMPLNT_FR_DT', 'MM/dd/yyyy')))

# Dataset
crime_data = arrests.union(complaints)




# Preprocessing demograhics data
keep_column = ['2006', '2007', '2008', '2009', '2010',
       '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']

poverty_rate_df = pd.read_csv("hdfs://ahalya/raw_data/sub-borougharea-povertyrate.csv").drop(columns=['short_name','long_name'])
poverty_rate_df.rename(columns={'Sub-Borough Area':'Borough'}, inplace=True)
poverty_rate = pd.melt(poverty_rate_df, id_vars=['Borough'], value_vars = keep_column)
poverty_rate = poverty_rate.rename(columns={'variable':'Year', 'value': 'proverty_rate'})

income_div_df = pd.read_csv("hdfs://ahalya/raw_data/sub-borougharea-incomediversityratio.csv").drop(columns=['short_name','long_name'])
income_div_df.rename(columns={'Sub-Borough Area':'Borough'}, inplace=True)
income_div = pd.melt(income_div_df, id_vars=['Borough'], value_vars = keep_column)
income_div = income_div.rename(columns={'variable':'Year', 'value': 'income_diversity'})

racial_div_df = pd.read_csv("hdfs://ahalya/raw_data/sub-borougharea-racialdiversityindex.csv").drop(columns=['short_name','long_name'])
racial_div_df.rename(columns={'Sub-Borough Area':'Borough'}, inplace=True)
racial_div = pd.melt(racial_div_df, id_vars=['Borough'], value_vars = keep_column)
racial_div = racial_div.rename(columns={'variable':'Year', 'value': 'racial_diversity'})

unemployment_df = pd.read_csv("hdfs://ahalya/raw_data/sub-borougharea-unemploymentrate.csv").drop(columns=['short_name','long_name'])
unemployment_df.rename(columns={'Sub-Borough Area':'Borough'}, inplace=True)
unemployment = pd.melt(unemployment_df, id_vars=['Borough'], value_vars = keep_column)
unemployment = unemployment.rename(columns={'variable':'Year', 'value': 'unemployment_rate'})

pop_df = pd.read_csv("hdfs://ahalya/raw_data/sub-borougharea-population.csv").drop(columns=['short_name','long_name'])
pop_df.rename(columns={'Sub-Borough Area':'Borough'}, inplace=True)
pop = pd.melt(pop_df, id_vars=['Borough'], value_vars = keep_column)
pop = pop.rename(columns={'variable':'Year', 'value': 'population'})

# Joining the datasets
merge1 = pd.merge(poverty_rate, income_div, how='inner', on=["Borough", "Year"])
merge2 = pd.merge(racial_div, unemployment, how='inner', on=["Borough", "Year"])
merge3 = pd.merge(merge2, pop, how='inner', on=["Borough", "Year"])
demographics_df = pd.merge(merge1, merge3, how='inner', on=["Borough", "Year"])
demographics_df["Borough"] = demographics_df["Borough"].str.upper() 
demographics_df["Demo_Key"] = demographics_df["Borough"]+demographics_df["Year"]

demographics_df.to_csv("hdfs://ahalya/raw_data/borough_demographics.csv",  index=False )




# Preparing data for linear regression

# Finding borough level aggregate
complaints_df = complaints.filter(col("Year").isin(keep_column))




complaints_df.dtypes




from pyspark.sql.types import IntegerType, StructField, StructType
from geospark.sql.types import GeometryType
import geopandas as gpd
from pyspark.sql import SparkSession
from geospark.register import GeoSparkRegistrator


points = gpd.read_file("NYC_Shape.shp")




points_geom = spark.createDataFrame(
    points[["bor_subb","name","geometry"]].astype(str)
)




complaints_df.take(2)




import pyspark.sql.functions as f
from pyspark.sql.functions import concat, col, lit
complaints_df = complaints_df.filter(complaints.Lat_Lon.isNotNull())
complaints_df = complaints_df.filter(complaints.Latitude.isNotNull())
complaints_df = complaints_df.filter(complaints.Longitude.isNotNull())
complaints_df = complaints_df.withColumn("LatLon",f.concat(lit("POINT("), f.col("Longitude"),lit(" "),f.col("Latitude"), lit(")")))
complaints = complaints_df.limit(1000)




colsToKeep = ['CMPLNT_FR_DT','CMPLNT_FR_TM','ADDR_PCT_CD','Latitude','Longitude','Year', 'LatLon']
complaints = complaints[colsToKeep]
complaints = complaints.na.drop(subset = ['CMPLNT_FR_DT','CMPLNT_FR_TM','ADDR_PCT_CD','Latitude','Longitude','Year'])
complaints.createOrReplaceTempView("county")
complaints.count()




points_geom.na.drop(subset=['bor_subb','name','geometry'])
points_geom.createOrReplaceTempView("pts")
points_geom.dtypes




from pyspark.sql import SparkSession
from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator
upload_jars()
GeoSparkRegistrator.registerAll(spark)
counties_geom = spark.sql(
    "SELECT *, st_geomFromWKT(LatLon) as point from county"
)
pts = spark.sql(
    "SELECT *,st_geomFromWKT(geometry) as shape from pts "
)




counties_geom.createOrReplaceTempView("counties")
pts.createOrReplaceTempView("pois")




output = spark.sql(
    """
        SELECT c.CMPLNT_FR_DT,c.CMPLNT_FR_TM,c.ADDR_PCT_CD,c.Latitude,c.Longitude, p.bor_subb,p.name
        FROM pois AS p, counties AS c
        WHERE p.shape IS NOT NULL AND c.point is not null and st_intersects(p.shape, c.point) limit 1
    """
).collect()




complaints_df.dtypes




#Preparing data for Regression
complaints_df = output
complaints_df.withColumn("KeyNew",fn.concat(fn.col("name"),fn.col("Year")))
crime_count = complaints_df.groupby("KeyNew").agg(fn.count(col('CMPLNT_NUM')).alias('crime_count'))


# Preprocessing in borough level data
demographics = spark.read.option("header", "true").csv("hdfs://ahalya/raw_data/borough_demographics.csv")
demographics = demographics.withColumn("unemployment_rate", demographics["unemployment_rate"].cast(DoubleType()))
demographics = demographics.withColumn("racial_diversity", demographics["racial_diversity"].cast(DoubleType()))
demographics = demographics.withColumn("income_diversity", demographics["income_diversity"].cast(DoubleType()))
demographics = demographics.withColumn("proverty_rate", demographics["proverty_rate"].cast(DoubleType()))
demographics = demographics.withColumn("population", demographics["population"].cast(DoubleType()))

# Combining Data
final_data = crime_count.join(demographics, crime_count.KeyNew == demographics.Demo_Key)
final_data = final_data.drop('Demo_Key')




# Correlation analysis
for i in final_data.columns:
    if not( isinstance(final_data.select(i).take(1)[0][0], six.string_types)):
        print( "Correlation to crime_count for ", i, final_data.stat.corr('crime_count',i))




# Utility functions

def linear_regression_inbuilt(data, dependent_vars):
    vectorAssembler = VectorAssembler(inputCols = dependent_vars, outputCol = 'features')
    vector_df = vectorAssembler.transform(data)
    vector_df = vector_df.select(['features', 'crime_count'])
#     glr = GeneralizedLinearRegression(family="binomial", link="logit", maxIter=10, regParam=0.0)
#     lr_model = glr.fit(data)
    lr = LinearRegression(featuresCol = 'features', labelCol='crime_count', maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(vector_df)
    return lr_model

def linear_regression(data, indexes):
    x = []
    y = []
    for each in data:
        x_s = [ float(each[index]) for index in indexes if each[index] != None]
        if len(x_s) == len(indexes):
            x.append(x_s)
            y.append(float(each[1]))
            
    N = len(x)
    M = len(indexes)
    df = N - (1+M)
    X = np.reshape(x,(N,M))
    Y = np.reshape(y,(len(y),1))
    
    if df <= 0:
        return -1
    
    if N > 1:
        X = (X - np.mean(X,axis=0))/np.std(X,axis=0)
        Y = (Y - np.mean(Y))/ np.std(Y)
    
    X = np.hstack((X,np.ones((N,1))))
    X_t = np.transpose(X)
    X_inv = np.linalg.pinv(np.dot(X_t,X))
    weights = np.dot( np.dot(X_inv,X_t) ,Y)
    
    rss = np.sum(np.power((Y - np.dot(X, weights)), 2))
    s_squared = rss / df
    se = np.sum(np.power((X[:, 0]), 2))
    tt = (weights[0, 0] / np.sqrt(s_squared / se))
    
    pval = stats.t.sf(np.abs(tt), df) 
    
    return weights[0][0],pval




# .coefficients, lr_model.intercept lr_model.summary 
                                                   
lr_model = linear_regression_inbuilt(final_data, ["unemployment_rate"])
model_summary = lr_model.summary
print("r2: %f" % model_summary.r2)




result = final_data.rdd.map(list).collect()

# significance of variables with crime_rate 
print("crime_rate vs proverty_rate "+str(linear_regression(result, [4])[1]))
print("crime_rate vs income_diversity "+ str(linear_regression(result, [5])[1]))
print("crime_rate vs racial_diversity "+str(linear_regression(result, [6])[1]))
print("crime_rate vs unemployment_rate "+str(linear_regression(result, [7])[1]))

#  significance of variables with crime_rate controlled by population
print("crime_rate vs proverty_rate controlled by population "+str(linear_regression(result, [4,8])[1]))
print("crime_rate vs income_diversity controlled by population "+ str(linear_regression(result, [5,8])[1]))
print("crime_rate vs racial_diversity controlled by population "+str(linear_regression(result, [6,8])[1]))
print("crime_rate vs unemployment_rate controlled by population  "+str(linear_regression(result, [7,8])[1]))

