from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName('spark-main-iris').getOrCreate()

    data = [
        ("Alice", "HR", 3500),
        ("Bob", "IT", 4200),
        ("Charlie", "IT", 3800),
        ("Diana", "Finance", 4000),
        ("Ethan", "HR", 3100),
        ("Fiona", "Marketing", 3600),
        ("George", "Finance", 4500),
        ("Hannah", "Sales", 3900),
        ("Ian", "Marketing", 2800),
        ("Julia", "Sales", 4300)
    ]
    columns = ["employee_name", "department", "salary"]
    df = spark.createDataFrame(data, columns)
    df.printSchema()
    df.show()
