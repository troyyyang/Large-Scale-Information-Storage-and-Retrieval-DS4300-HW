
// Read temperature data for 1986 and station data

val temps = spark.read.csv("weather/1986.csv").toDF("STATION", "WBAN", "MONTH", "DAY", "TEMP")
val stations = spark.read.csv("weather/stations.csv").toDF("STATION", "WBAN", "LAT", "LON")

// Filter bad Stations where the latitude or longitude is unavailable
// Only include temperature data for JANUARY 28th, 1986
val stas = stations.select(stations("station"), stations("wban"), stations("lat").cast("double"), stations("lon").cast("double")).filter($"LAT".isNotNull && $"LON".isNotNull)
val tmps = temps.filter($"month"=== 1 && $"day" === 28)

// Define a function that compute the distance between two points on the Earth using the Haversine formula

val pi = 3.14159265
val REarth = 6371.0 // kilometers
def toRadians(x: Double): Double = x * pi / 180.0

def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dlon = toRadians(lon2-lon1) 
    val dlat = toRadians(lat2-lat1)
    
    val a = math.pow(math.sin(dlat/2), 2) + math.cos(toRadians(lat1)) * math.cos(toRadians(lat2)) * math.pow(math.sin(dlon/2), 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    REarth * c
    
}

import org.apache.spark.sql.functions.udf
val haver = udf(haversine _)

// Find all stations within 100 km using haversine function

val capeCanaveralLatitude = 28.388382
val capeCanaveralLongitude = -80.603498

val st100 = stas.filter(haver($"lat", $"lon", lit(capeCanaveralLatitude), lit(capeCanaveralLongitude)) <= 100)

// Use inverse distance weighting to estimate the temperature at Cape Canaveral on that day

import org.apache.spark.sql.functions.pow


val stidw = st100.withColumn("idw", pow(haver($"lat", $"lon", lit(capeCanaveralLatitude), lit(capeCanaveralLongitude)), -2))


val stnonull = stidw.na.fill(Map("wban" -> 0, "station" -> 0))
val tmpnonull = tmps.na.fill(Map("wban" -> 0, "station" -> 0))

val complete = stnonull.join(tmpnonull, stnonull("wban") === tmpnonull("wban") && stnonull("station") === tmpnonull("station"))

complete.agg(sum($"idw" * $"temp")/sum($"idw")).head.getDouble(0)


// Extra credit - find average temperature for Jan 28 for every year.
// Generate a line plot.
// Was the Jan 28, 1986 temperature unusual?

import scala.collection.mutable.ListBuffer


var jan28temps = new ListBuffer[Double]()


for ( i <- 1975 to 2015) {
    val file = "weather/" + i.toString + ".csv"
    val yeartemp = spark.read.csv(file).toDF("STATION", "WBAN", "MONTH", "DAY", "TEMP").filter($"month"=== 1 && $"day" === 28).na.fill(Map("wban" -> 0, "station" -> 0))
    val complete = stnonull.join(yeartemp, stnonull("wban") === yeartemp("wban") && stnonull("station") === yeartemp("station"))
    jan28temps += complete.agg(sum($"idw" * $"temp")/sum($"idw")).head.getDouble(0)
}


print(jan28temps.toList)

// it appears that it was unusually cold on jan 28 1986
// unfortunatley I couldn't figure out how to install libraries/manage dependencies with scala, so I couldn't create a graph

//import co.theasi.plotly

//val xs = (1975 until 2015)

//val p = Plot().withScatter(xs, jan28temps.toList)

//draw(p, "basic-scatter", writer.FileOptions(overwrite=true))