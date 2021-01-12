import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType};
import org.apache.spark.sql.functions._


object app {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("SpatialJoin").getOrCreate()
    import spark.implicits._

    val start_time = System.currentTimeMillis()

    //Input arguments
    val d = args(0).toDouble
    val num_partitions = args(1).toInt
    val hotels = args(2)
    val restaurants = args(3)

    //Creating the schemas for each datasets
    val schema1 = new StructType().add("RecordNumber",IntegerType,true).
      add("Name",StringType,true).
      add("col1",StringType,true).
      add("col2",StringType,true).
      add("Lat",DoubleType,true).
      add("Lon",DoubleType,true).
      add("Comments",StringType,true)

    val schema2 = new StructType().add("RecordNumber",IntegerType,true).
      add("Name",StringType,true).
      add("col1",StringType,true).
      add("Lat",DoubleType,true).
      add("Lon",DoubleType,true).
      add("Comments",StringType,true)

    // Loading the datasets
    val  hotels_df = spark.read.format("csv").option("delimiter","|").schema(schema1).
      load(hotels).drop("col1").drop("col2").drop("comments").drop("RecordNumber")
    val  restaurants_df = spark.read.format("csv").option("delimiter","|").schema(schema2).
      load(restaurants).drop("col1").drop("Comments").drop("RecordNumber")

    //Find min and max Latitude and Longitude between the two datasets

    val min_max_hotels = hotels_df.agg(min("Lat"),max("Lat"),min("Lon"),max("Lon")).head()
    val min_max_restaurants = restaurants_df.agg(min("Lat"),max("Lat"),min("Lon"),max("Lon")).head()

    val min_Lat  = min_max_restaurants.getDouble(0) min min_max_hotels.getDouble(0)
    val max_Lat  = min_max_restaurants.getDouble(1) max min_max_hotels.getDouble(1)

    //Create Partitions as a List of Vectors(Double,Double)
    val step = (max_Lat-min_Lat)/num_partitions
    val partitions = (min_Lat until max_Lat by step) map {i => (i , i + step)}


    //------------------------------------------------------------------------------------
    //Haversine distance formula

    def haversine(star_lat:Double,start_long:Double,end_lat:Double,end_long:Double): Double = {
      val earth_radius = 6371
      val lat_distance = Math.toRadians(star_lat - end_lat)
      val long_distance = Math.toRadians(start_long - end_long)
      val sinLat = Math.sin(lat_distance / 2)
      val sinLng = Math.sin(long_distance / 2)
      val a = sinLat * sinLat +(Math.cos(Math.toRadians(star_lat)) * Math.cos(Math.toRadians(end_lat)) * sinLng * sinLng)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      (earth_radius* c).toDouble
    }

    //Make the function as a udf
    val haversine_udf = udf(haversine _)

    //------------------------------------------------------------------------------------
    // Check if an input value is between (start,end)
    val in_range = (x1: Double, x2: Double, x : Double) => (x >= x1 && x < x2)

    // Find in which index of the partition list is an input value (Latitude)
    val find_index = udf((x:Double) => {
      partitions.indexWhere(v => in_range(v._1, v._2, x))
    })

    // Find in which index of the partition list is an input value (Latitude)
    // and if it is close enough to the boundary so that it should be copied to the next one later
    val find_indexes = udf((lat:Double,long:Double) => {
      val index1 = partitions.indexWhere(v => in_range(v._1, v._2, lat))
      var index2 = index1
      val distance_from_bound = haversine(lat,long,partitions(index1)._2,long)
      if (distance_from_bound <= d) index2 += 1
      if ( index2 == num_partitions) index2 -= 1
      Array(index1,index2)
    })

    //------------------------------------------------------------------------------------

    val hotels_df2 = hotels_df.withColumn("partition", find_indexes(hotels_df("Lat"),hotels_df("Lon"))).
      withColumn("partition1", $"partition"(0)).withColumn("partition2", $"partition"(1))

    //Create a new df by duplicating the points that are close to the boundary lines
    val new_hotels_df2 = hotels_df2.drop("partition").drop("partition2").withColumnRenamed("partition1","partition").
      union(hotels_df2.where($"partition2" > $"partition1").drop("partition").drop("partition1").withColumnRenamed("partition2","partition")).
      repartitionByRange(num_partitions,$"partition")

    val restaurants_df2 = restaurants_df.withColumn("partition", find_index(restaurants_df("Lat"))).repartitionByRange(num_partitions,$"partition")


    val joined_df = new_hotels_df2.join(restaurants_df2,haversine_udf(new_hotels_df2("Lat"),new_hotels_df2("Lon"),restaurants_df2("Lat"),restaurants_df2("Lon")) <= d ).
      toDF("Hotel","Hotel_Lat","Hotel_Lon","Hotel_Partition","Restaurant","Restaurant_Lat","Restaurant_Lon","Restaurant_Partition")

    joined_df.show()

    val end_time = System.currentTimeMillis()
    val total_time = end_time - start_time
    println(s"Distance = $d , Partitions = $num_partitions, Time = $total_time")


  }
}
