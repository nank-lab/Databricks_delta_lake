# CREATE STREAMING TABLE

@dlt.table(
name= "first_stream_table_v2"
)
def first_stream_table():
return spark.readStream.table("dltnandhini.source.orders")

# CREATE MATERIALIZED VIEW

@dlt.table(
name="first_mat_view"
)
def first_mat_view():
return spark.read.table("dltnandhini.source.orders")

# CREATE VIEW --holding queries
@dlt.view(
name="first_batch_view"
)
def first_batch_view():
return spark.read.table("dltnandhini.source.orders")

# CREATE STEAMING VIEW
@dlt.table(
name="first_stream_view"
)
def first_stream_view():
return spark.readStream.table("dltnandhini.source.orders")

