// Databricks notebook source
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "fa66d22f-fae8-4051-993b-6211926c8fbe",
  "fs.azure.account.oauth2.client.secret" -> "3ID3Tty6Lj3?taY=IDvI/DqG:l=2.8HC",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/237f75fc-468c-4b2f-87af-eac1aef2df40/oauth2/token")

// COMMAND ----------

dbutils.fs.unmount(mountPoint = "/mnt/data")
dbutils.fs.mount(
source = "abfss://data@databricks2soruce.dfs.core.windows.net",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  
  (total - discount).asInstanceOf[Int]
}

// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

// COMMAND ----------

spark.udf.register("getCustomerType", getCustomerType)
spark.udf.register("getOrderAmount", getOrderAmount)


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
// MAGIC     c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
// MAGIC     getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
// MAGIC     p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
// MAGIC     o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
// MAGIC   FROM PracticeDB.Orders o
// MAGIC   INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
// MAGIC   INNER JOIN PracticeDB.Products p ON p.productid = o.product
// MAGIC   WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
// MAGIC   ORDER BY OrderAmount