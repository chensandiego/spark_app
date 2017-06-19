from pyspark import SparkContext

sc=SparkContext("local[2]","first app")

data=sc.textFile("data/UserPurchaseHistory.csv").map(lambda line: line.split(",")).map(lambda record: (record[0],record[1],record[2]))

#count the number of purchases
numPurchases=data.count()


#get only unique users
uniqueUsers=data.map(lambda record: record[0]).distinct().count()


#sum up total revenue
totalRevenue=data.map(lambda record: float(record[2])).sum()


#find out popular products
products=data.map(lambda record: (record[1],1.0)).reduceByKey(lambda a,b: a + b).collect()

mostPopular=sorted(products,key=lambda x: x[1],reverse=True)[0]



print("total purchase: %d" %numPurchases)

print("Unique Users: %d" %uniqueUsers)
print("total revenue: %2.2f" %totalRevenue)

print("most popular product: %s with %d purchases" % (mostPopular[0],mostPopular[1]))
