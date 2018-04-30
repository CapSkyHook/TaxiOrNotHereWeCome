// Code take from https://dzone.com/articles/scala-calculating-distance-between-two-locations
package runtime
case class Location(lat: Double, lon: Double)
trait DistanceCalculator {
    def calculateDistanceInKilometer(locationOne: Location, locationTwo: Location): Double
}
class DistanceCalculatorImpl extends DistanceCalculator {
    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    override def calculateDistanceInKilometer(locationOne: Location, locationTwo: Location): Double = {
        val latDistance = Math.toRadians(locationOne.lat - locationTwo.lat)
        val lngDistance = Math.toRadians(locationOne.lon - locationTwo.lon)
        val sinLat = Math.sin(latDistance / 2)
        val sinLng = Math.sin(lngDistance / 2)
        val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(locationOne.lat)) *
            Math.cos(Math.toRadians(locationTwo.lat)) *
            sinLng * sinLng)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        (AVERAGE_RADIUS_OF_EARTH_KM * c).toDouble
    }
}
