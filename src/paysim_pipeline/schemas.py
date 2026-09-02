"""Explicit Spark schemas used by the PaySim pipeline."""

from pyspark.sql import types as T


RAW_TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("step", T.IntegerType(), False),
        T.StructField("type", T.StringType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("nameOrig", T.StringType(), False),
        T.StructField("oldbalanceOrg", T.DoubleType(), False),
        T.StructField("newbalanceOrig", T.DoubleType(), False),
        T.StructField("nameDest", T.StringType(), False),
        T.StructField("oldbalanceDest", T.DoubleType(), False),
        T.StructField("newbalanceDest", T.DoubleType(), False),
        T.StructField("isFraud", T.IntegerType(), False),
        T.StructField("isFlaggedFraud", T.IntegerType(), False),
    ]
)


BRONZE_REQUIRED_COLUMNS = [
    "step",
    "transaction_type",
    "amount",
    "origin_account",
    "origin_old_balance",
    "origin_new_balance",
    "destination_account",
    "destination_old_balance",
    "destination_new_balance",
    "is_fraud",
    "is_flagged_fraud",
]


SILVER_REQUIRED_COLUMNS = BRONZE_REQUIRED_COLUMNS + [
    "transaction_day",
    "transaction_hour",
    "is_high_value_transaction",
    "origin_balance_error",
    "destination_balance_error",
]
