import dlt
from pyspark.sql.functions import *
@dlt.table
def dimuser_stg():
    df = spark.readStream.table("spotify_cat.silver.dimuser")
    return df


from pyspark import pipelines as dp

dp.create_auto_cdc_flow(
  target = "spotify_cat.gold.dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  ignore_null_updates = False,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = "cdc_dimuser",
  once = False
)