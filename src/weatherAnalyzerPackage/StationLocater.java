package weatherAnalyzerPackage;

import java.lang.Math;

/*
 *  [x] take in lat /long coordinates as Doubles or Strings
 *  [x] Convert to doubles if needed
 *  [ ] Compare lat/longs to identify region that station is in
 *      returns status code for checking if station found.
 *  [ ] Set all attributes of the object
 *  
 */
public class StationLocater {
  private String name = "XX";
  private String state = "XX";
  private String country = "XX";
  Double latitude = 9999.9; // in degrees
  Double longitude = 9999.9; // in degrees
  
  public int findStation(Double coodLat, Double coodLong ) {
    
    latitude = coodLat;
    longitude = coodLong;
    
    return this.findNearestStation();
  }
  
  public int findStation(String coodLat, String coodLong ) {

    latitude = Double.parseDouble(coodLat);
    latitude = Double.parseDouble(coodLong);
    
    return this.findNearestStation();
  }
  
  public int findNearestStation() {
    /*
     * The function that is the engine for setting all the attributes
     * for the station object
     */
    return 0;
  }
  
  public Double findDistance (Double coodLat, Double coodLong) {
    
    // ‘haversine’ formula
    // http://www.movable-type.co.uk/scripts/latlong.html
    Double RADIUS = 6371000.0; // Radius of Earth in meters
    Double originLatRad = deg2rad(this.latitude);
    Double coodLatRad = deg2rad(coodLat);
    Double deltaLat = deg2rad(coodLat - this.latitude); // (lat2-lat1).toRadians();
    Double deltaLong = deg2rad(coodLong - this.longitude); // (lon2-lon1).toRadians();

    Double a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) +
            Math.cos(originLatRad) * Math.cos(coodLatRad) *
            Math.sin(deltaLong / 2.0) * Math.sin(deltaLong / 2.0);
    Double c = 2 * Math.atan2( Math.sqrt(a), Math.sqrt(1 - a));
    
    return RADIUS * c;
  }
  
  public Double deg2rad(Double degrees) {
    return degrees * Math.PI / 180.0;
  }
  
  public String getName() {
    return name;
  }
  
  public String getState() {
    return state;
  }
  
  public String getCountry() {
    return country;
  }
  
  public Double getLat() {
    return latitude;
  }
  
  public Double getLong() {
    return longitude;
  }
  
  public int setName(String n) {
    name = n;
    return 0;
  }
  
  public int setState(String s) {
    state = s;
    return 0;
  }
  
  public int setCountry(String c) {
    country = c;
    return 0;
  }
  
  public int setLat(Double coodLat) {
    latitude = coodLat;
    return 0;
  }
  
  public int setLong(Double coodLong) {
    longitude = coodLong;
    return 0;
  }
  
  
}
