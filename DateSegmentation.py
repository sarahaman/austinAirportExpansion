from pyspark.sql.functions import udf, col

# Columns for Dates
DateColumns = ['YEAR', 'MONTH', 'DAY_OF_WEEK', 'FL_DATE']

OriginCols = ['ORIGIN_AIRPORT_ID', 'ORIGIN_CITY_MARKET_ID', 'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR']
DestinationCols = ['DEST_AIRPORT_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR']

DistanceCols = ['DISTANCE', 'DISTANCE_GROUP']

CancelDelayCols = ['DEP_DELAY', 'DEP_DELAY_NEW', 'DEP_DEL15', 'DEP_DELAY_GROUP', 'ARR_DELAY', 'ARR_DELAY_NEW', 'ARR_DEL15', 'ARR_DELAY_GROUP', 'CANCELLED', 'CANCELLATION_CODE', 'DIVERTED', 'TAXI_IN', 'TAXI_OUT']


def assignQuarter(year, month):
    '''
    Function for assigning quarterly values based on the austin bergstrom fiscal calendar (ends on September 30)
    '''
    switcher = {
        1: "Q2",
        2: "Q2",
        3: "Q2",
        4: "Q3",
        5: "Q3",
        6: "Q3",
        7: "Q4",
        8: "Q4",
        9: "Q4",
        10: "Q1",
        11: "Q1",
        12: "Q1"
    }
    Quarter = switcher.get(month)
    if Quarter == "Q1":
        year += 1
    return ' '.join([str(year), Quarter])

quarterUDF = udf(lambda yr,mo: assignQuarter(yr,mo))

#df.withColumn("Quarter", quarterUDF(col("YEAR"), col("MONTH")))
