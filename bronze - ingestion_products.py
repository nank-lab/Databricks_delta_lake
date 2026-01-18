import dlt
from pyspark.sql.functions import *

products_rules = {
    "rule_1": "product_id IS NOT NULL",
    "rule_2": "price >= 0"
}

@dlt.table(
    name="products_stg"
)
@dlt.expect_all_or_drop(products_rules)
def products_stg():
    df = spark.readStream.table("dltnandhini.source.products")
    return df
