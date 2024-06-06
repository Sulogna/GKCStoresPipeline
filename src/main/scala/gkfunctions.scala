import org.apache.spark.sql.types._

object gkfunctions {

  //Method to read schema
  def read_schema(schema_arg : String) ={
    var sch : StructType = new StructType
    //From the landingFileSchema we are taking all the values spliting by "," and storing it in a list
    val split_values = schema_arg.split(",").toList

    val d_types = Map(
      "StringType" -> StringType,
      "IntegerType" -> IntegerType,
      "TimestampType" -> TimestampType,
      "DoubleType" -> DoubleType
    )

    for(i <- split_values){
      //inside the list we are taking each value and spliting by " " space to get Sale_ID and StringType separately
      val columnVal = i.split(" ").toList
      //the value that we are getting we will keep appending into our blank schema
      //add function on structType automatically binds them into StructField we don't have to mention that separately
      sch = sch.add(columnVal(0), d_types(columnVal(1)), true)
      //Now we are getting second index as string "StringType" and not as datatype for that we can use scala map
    }

    //returning schema
    sch

  }

}
