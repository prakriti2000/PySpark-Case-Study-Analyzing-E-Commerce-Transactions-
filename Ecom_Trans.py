from pyspark.sql import SparkSession

# Initialize SparkSession
if __name__ == "__main__":
    #creating a spark session
    spark = SparkSession.builder.appName("E-Commerce Analysis").getOrCreate()

    # Get SparkContext from SparkSession
    sc = spark.sparkContext

    # Sample data
    data = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]

    transactions_rdd = sc.parallelize(data)

    transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))

    high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)

    products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])

    pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))

    customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)

    customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)

    # Define the product_category_rdd for joining with transaction data
    product_category_data = [
        ('Laptop', 'Electronics'),
        ('Headphones', 'Electronics'),
        ('Book', 'Books'),
        ('Chair', 'Furniture')
    ]

    product_category_rdd = sc.parallelize(product_category_data)

    customer_product_category_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(product_category_rdd)

    total_spending = customer_spending_rdd.collect()
    print(total_spending)

    products_per_customer = customer_products_rdd.collect()
    print(products_per_customer)

    product_category_info = customer_product_category_rdd.collect()
    print(product_category_info)

    # Save the results to a text file
    customer_spending_rdd.saveAsTextFile("file:///home/takeo/pycharmProjects/pythonProject/output/customer_spending")
    customer_products_rdd.saveAsTextFile("file:///home/takeo/pycharmProjects/pythonProject/output/customer_products")
    customer_product_category_rdd.saveAsTextFile(
        "file:///home/takeo/pycharmProjects/pythonProject/output/customer_product_category")
