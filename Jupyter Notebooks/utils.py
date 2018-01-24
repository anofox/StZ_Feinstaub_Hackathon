import findspark
findspark.init()

import math
import datetime as dt
import holidays
import geohash

from pyspark import keyword_only
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param.shared import HasInputCols, HasInputCol, HasOutputCol

class TimestampTransformer(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(TimestampTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self.day_to_str = {0: "Monday",
                           1: "Tuesday",
                           2: "Wednesday",
                           3: "Thursday",
                           4: "Friday",
                           5: "Saturday",
                           6: "Sunday"}
        self.bins = 24 * 4 # number of time bins per day
        # Note: bins must evenly divide 60
        self.minutes_per_bin = int((24. / float(self.bins)) * 60.)
        self.holidays = holidays.Germany(prov="BW")

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def datedim_schema(self):
        return StructType([
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("day", IntegerType()),
            StructField("day_of_week", IntegerType()),
            StructField("day_of_year", IntegerType()),
            StructField("weekend", IntegerType()),
            StructField("holiday", IntegerType()),
            StructField("day_cat", StringType()),
            StructField("day_num", DoubleType()),
            StructField("day_cos", DoubleType()),
            StructField("day_sin", DoubleType()),
            StructField("time_cat", StringType()),
            StructField("time_bin", IntegerType()),
            StructField("hour_bin", IntegerType()),
            StructField("min_bin", IntegerType()),
            StructField("time_num", DoubleType()),
            StructField("time_cos", DoubleType()),
            StructField("time_sin", DoubleType()),
        ])
    
    def datedim_values(self, dt):
        # Calculate number of minute into the day (eg. 1016)
        num_minutes = dt.hour * 60. + dt.minute
    
        # Time of the start of the bin
        time_bin = math.floor(num_minutes / self.minutes_per_bin)                  # eg. 1005
        hour_bin = math.floor(num_minutes / 60.)                                   # eg. 16
        min_bin = (time_bin * self.minutes_per_bin) % 60                           # eg. 45
        
        # Get a floating point representation of the center of the time bin
        time_num = (hour_bin*60. + min_bin + self.minutes_per_bin / 2.0)/(60.*24.) # eg. 0.7065972222222222
        time_cos = math.cos(time_num * 2 * math.pi)
        time_sin = math.sin(time_num * 2 * math.pi)
        
        #get time_cat
        time_cat = "%02d:%02d" % (hour_bin, min_bin)                               # eg. "16:45"
        
        day_of_week = dt.weekday()
        day_of_year = dt.timetuple().tm_yday
        day_cat = self.day_to_str[day_of_week]
        day_num = (day_of_week + time_num) / 7.
        day_cos = math.cos(day_num * 2. * math.pi)
        day_sin = math.sin(day_num * 2. * math.pi)
        
        year = dt.year
        month = dt.month
        day = dt.day
    
        weekend = 0
        if day_of_week in [5,6]:
            weekend = 1
            
        holid = 0
        if dt in self.holidays:
            holid = 1
            
        return {
            "year": year, 
            "month": month, 
            "day": day,
            "day_of_week": day_of_week,
            "day_of_year": day_of_year,
            "weekend": weekend,
            "holiday": holid,
            "day_cat": day_cat,
            "day_num": day_num,
            "day_cos": day_cos,
            "day_sin": day_sin,
            "time_cat": time_cat,
            "time_bin": time_bin,
            "hour_bin": hour_bin,
            "min_bin":  min_bin,
            "time_num": time_num,
            "time_cos": time_cos,
            "time_sin": time_sin,
        }
    
    def _transform(self, dataset):
        datedim_udf = udf(lambda dt: self.datedim_values(dt), self.datedim_schema())
        
        in_col = self.getInputCol()
        out_col = self.getOutputCol()

        dataset = dataset.withColumn(out_col, datedim_udf(in_col))
        
        return dataset

    
class GeohashTransformer(Transformer, HasInputCols, HasOutputCol):
    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(GeohashTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self.bb_prefix = 8

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    def geohash_schema(self):
        return StructType([
            StructField("hash", StringType()),
            StructField("hash_u64", StringType()),
            StructField("hash_bin", StringType()),
        ])
    
    def geohash_values(self, lat, lon):
        hsh = geohash.encode(lat, lon) if lat and lon else None
        
        return {
           "hash": hsh,
           "hash_u64": geohash.encode_uint64(lat, lon) if lat and lon else None,
           "hash_bin": hsh[:self.bb_prefix] if hsh else None 
        }
        
    def _transform(self, dataset):
        geohash_udf = udf(lambda lat, lon: self.geohash_values(lat, lon), self.geohash_schema())

        in_cols = self.getInputCols()
        out_col = self.getOutputCol()
        
        dataset = dataset.withColumn(out_col, geohash_udf(*in_cols))
        return dataset