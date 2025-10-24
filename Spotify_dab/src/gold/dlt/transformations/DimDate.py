import dlt
from pyspark.sql.functions import *
@dlt.table
def dimdate_stg():
    df = spark.readStream.table("spotify_cat.silver.dimdate")
    return df


from pyspark import pipelines as dp

dp.create_auto_cdc_flow(
  target = "spotify_cat.gold.dimdate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = "date",
  ignore_null_updates = False,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = "cdc_dimdate",
  once = False
)