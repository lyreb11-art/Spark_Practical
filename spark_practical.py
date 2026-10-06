from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================================
# SPARK TRANSFORMATIONS, ACTIONS, LAZY EVALUATION & DAG
# ==========================================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Sensor_DAG_Practical")
    .config("spark.python.worker.reuse", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n==========================================")
print("SPARK SESSION CREATED")
print("==========================================")

# ----------------------------------------------------------
# STEP 1: DEFINE SCHEMA
# ----------------------------------------------------------

schema = """
machine_id STRING,
ts STRING,
vibration DOUBLE,
temperature DOUBLE
"""

# ----------------------------------------------------------
# STEP 2: LOAD CSV DATASET
# ----------------------------------------------------------

df = spark.read.csv(
    "sensor_readings.csv",
    header=True,
    schema=schema
)

print("\n==========================================")
print("DATASET LOADED FROM CSV")
print("==========================================")

df.show(40, truncate=False)

print("\nTotal records:", df.count())

# ----------------------------------------------------------
# STEP 3: FILTER TRANSFORMATION
# ----------------------------------------------------------

print("\n==========================================")
print("TRANSFORMATION 1: FILTER")
print("==========================================")

hot = df.filter(
    F.col("temperature") > 80
)

print("Filter created: temperature > 80")
print("No action has been called yet.")

# ----------------------------------------------------------
# STEP 4: GROUPBY + AGGREGATION
# ----------------------------------------------------------

print("\n==========================================")
print("TRANSFORMATION 2: GROUPBY + AVG")
print("==========================================")

result = (
    hot.groupBy("machine_id")
       .agg(
           F.round(
               F.avg("vibration"), 2
           ).alias("avg_vibration")
       )
)

print("groupBy() and avg() created.")
print("No action has been called yet.")

# ----------------------------------------------------------
# STEP 5: LAZY EVALUATION
# ----------------------------------------------------------

print("\n==========================================")
print("LAZY EVALUATION")
print("==========================================")

print("Transformations only build the execution plan.")
print("Spark executes them when an ACTION is called.")

# ----------------------------------------------------------
# STEP 6: EXECUTION PLAN
# ----------------------------------------------------------

print("\n==========================================")
print("EXECUTION PLAN / DAG")
print("==========================================")

result.explain("formatted")

# ----------------------------------------------------------
# STEP 7: ACTION - SHOW
# ----------------------------------------------------------

print("\n==========================================")
print("ACTION 1: SHOW")
print("==========================================")

result.show()

# ----------------------------------------------------------
# STEP 8: ACTION - COUNT
# ----------------------------------------------------------

print("\n==========================================")
print("ACTION 2: COUNT")
print("==========================================")

hot_count = hot.count()

print("Number of hot readings:", hot_count)

# ----------------------------------------------------------
# STEP 9: KEEP SPARK RUNNING FOR WEB UI
# ----------------------------------------------------------

print("\n==========================================")
print("SPARK WEB UI")
print("==========================================")

print("Open your browser:")
print("http://localhost:4040")

print("\nInspect:")
print("1. Jobs")
print("2. Stages")
print("3. SQL / DAG")
print("4. Shuffle")
print("5. Stage details")

input("\nPress ENTER after inspecting Spark Web UI...")

# ----------------------------------------------------------
# STEP 10: STOP SPARK
# ----------------------------------------------------------

spark.stop()

print("\n==========================================")
print("SPARK APPLICATION STOPPED")
print("==========================================")