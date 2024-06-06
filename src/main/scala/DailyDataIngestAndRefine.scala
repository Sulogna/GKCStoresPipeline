import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config,ConfigFactory}
import gkfunctions.read_schema
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object DailyDataIngestAndRefine {

  def main(args: Array[String]): Unit = {

    // Creates a spark session
    // appName name that we want it to populate while running this job
    // We will be running this on HotandworksCluster but for testing we are defining it in local

    val spark = SparkSession.builder().appName("DailyDataIngestAndRefine").master("local[*]").getOrCreate()

    //Creates spark Context
    val sc = spark.sparkContext

    // Reading landing data from Config file
    val gkconfig : Config = ConfigFactory.load("application.conf")
    val inputLocation = gkconfig.getString("paths.inputLocation")
    val outputLocation = gkconfig.getString("paths.outputLocation")

    //Reading Schema from config
    val landingFileSchemaFromFile = gkconfig.getString("schema.landingFileSchema")
    val landingFileSchema = read_schema(landingFileSchemaFromFile)
    val holdFileSchemaFromFile = gkconfig.getString("schema.holdFileSchema")
    val holdFileSchema = read_schema(holdFileSchemaFromFile)





    //Spark implicits is useful when we want to convert one Data Structure to another in spark
    import spark.implicits._

    // Now we need to read the files
    // To read we need to first define a schema for this file ***
    // Spark also provides the option of infering the schema where if you provide the option of inferSchema as true,
    // Spark will by default analyse this dataset and see what datatype this could be but that is not idea.

    //Handling dates
    val dateToday = LocalDate.now() //today's date
    val yesterDate = dateToday.minusDays(1) //yesterday's date

    //Format date because we have the files in SalesDump_18072020 we only need the date
    val currDayZoneSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    //This gives us date in this format _05062024
    val prevDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    //Now our input file syntax in spark.read will change a little, in .csv(inputLocation + "SalesDump" + currDayZoneSuffix)

    //Creating the struct type schema
   /* val landingFileSchema = StructType(List(
      StructField("Sale_ID", StringType, true),
      StructField("Product_ID", StringType, true),
      StructField("Quantity_Sold", IntegerType, true),
      StructField("Vendor_ID", StringType, true),
      StructField("Sale_Date", TimestampType, true),
      StructField("Sale_Amount", DoubleType, true),
      StructField("Sale_Currency", StringType, true),
    ))*/
    // Read schema from config file instead of hardcoding schema here


    //Reading the file
    val landingFileDF = spark.read
      .schema(landingFileSchema)
      .option("delimiter", "|")
      .csv(inputLocation + "Sales_Landing/SalesDump" + currDayZoneSuffix)
    landingFileDF.createOrReplaceGlobalTempView("landingFileDF") //to use spark sql

    landingFileDF.show()
    landingFileDF.printSchema()
    //Avoid hardcoding path to read

    //along with today's valid data we also need to read previous day's hold data(previous day's invalid data)
    //if there is a null value for the same record in the current day's record but had value in previous day we consider previous day's value for that column
    //if the value for sales is changing in the current day's record then we consider latest record

    //Checking if updated were received on any previous hold data
    val previousHoldDf = spark.read
      .schema(holdFileSchema)
      .option("delimiter", "|")
      .option("header", true) //because when writing to HoldData we already included the header
      .csv(outputLocation + "Hold/HoldData" + prevDayZoneSuffix)
    previousHoldDf.createOrReplaceGlobalTempView("previousHoldDf")
    previousHoldDf.show()

    //Reading Landing Data along with the previous hold data along with some logics that we need to perform to get the final data
    //to use any DF as a spark SQL that DF should be registered as a view
    //For computations inside our SQL we can use case statement inside which we can use WHEN and THEN clauses
    val refreshedLandingData = spark.sql("select a.Sale_ID, a.Product_ID, " +
      "CASE " +
      "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold ELSE a.Quantity_Sold " +
      "END AS Quantity_Sold " +
      "CASE " +
      "WHEN (a.Vendor_ID IS NULL) THEN b.VENDOR_ID ELSE a.VENDOR_ID " +
      "END AS Vendor_ID " +
      "a.Sale_Date, a.Sale_Amount, a.Sale_Currency " +
      "from landingFileDF a " +
      "LEFT OUTER JOIN previousHoldDf b" +
      " ON a.Sale_ID = b.Sale_ID")
    refreshedLandingData.createOrReplaceGlobalTempView("refreshedLandingData")

    //Business Use case 1 where if Vendor_ID or Quantity_sold is null we hold the data and do not process it the same day or until the correct values come
    val validLandingData = refreshedLandingData.filter(col("Quantity_Sold").isNotNull
      && col("Vendor_ID").isNotNull)
    validLandingData.createOrReplaceGlobalTempView("validLandingData")

    //Now our refreshedLandingData contains new data of current day and hold data from previous day (only the ones which had updates)
    //The hold data from previous day that still does not have updates need to be still held in the current hold stage to be processed later

    val releasedFromHold = spark.sql("SELECT vd.Sale_ID from validLandingData vd INNER JOIN previousHoldDf phd " +
    "ON vd.Sale_ID = phd.Sale_ID")
    releasedFromHold.createOrReplaceGlobalTempView("releasedFromHold")

    val notReleasedFromHold = spark.sql("SELECT * from previousHoldDf " +
      "WHERE Sale_ID NOT IN (select Sale_ID from releasedFromHold)")
    notReleasedFromHold.createOrReplaceGlobalTempView("notReleasedFromHold")

    //While taking invalid data we also need to include held data from previous day that is what is done with UNION()
    //We are also adding a new column to give us the Hold Reason, for conditional statements inside the withColumn we can use the when function
    //When is also part of Spark SQL functions
    //Until here the schema was same now after we have added column the schema will change so we need to add another schema for hold files
    val invalidLandingData = refreshedLandingData.filter(col("Quantity_Sold").isNull
      || col("Vendor_ID").isNull)
      .withColumn("Hold Reason", when(col("Quantity_Sold").isNull, "Qty Sold Missing")
      .otherwise(when(col("Vendor_ID").isNull, "Vendor ID is missing")))
      .union(notReleasedFromHold)


    //Here we are writing valid data in a "valid" folder with the date suffix
    validLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Valid/ValidData" + currDayZoneSuffix)

    //Here we are writing invalid data in an "invalid" folder with the date suffix
    invalidLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Hold/HoldData" + currDayZoneSuffix)



  }

}
