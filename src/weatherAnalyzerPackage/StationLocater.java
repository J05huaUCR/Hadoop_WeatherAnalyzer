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
import java.util.ArrayList;

public class StationLocater {

  class RefState{
    public String State;
    public double Lat;
    public double Long;
    public RefState(String s, double lat, double lon) {
      State = s;
      Lat = lat;
      Long = lon;
    }
  }

  ArrayList<RefState> LookupList;

  private String name = "XX";
  private String state = "XX";
  private String country = "XX";
  Double latitude = 9999.9; // in degrees
  Double longitude = 9999.9; // in degrees

  public StationLocater(){
    LookupList = new ArrayList<RefState>();

    LookupList.add(new RefState("AK",61.3850,-152.2683));
    LookupList.add(new RefState("AL",32.7990,-86.8073));
    LookupList.add(new RefState("AR",34.9513,-92.3809));
    LookupList.add(new RefState("AS",14.2417,-170.7197));
    LookupList.add(new RefState("AZ",33.7712,-111.3877));
    LookupList.add(new RefState("CA",36.1700,-119.7462));
    LookupList.add(new RefState("CO",39.0646,-105.3272));
    LookupList.add(new RefState("CT",41.5834,-72.7622));
    LookupList.add(new RefState("DC",38.8964,-77.0262));
    LookupList.add(new RefState("DE",39.3498,-75.5148));
    LookupList.add(new RefState("FL",27.8333,-81.7170));
    LookupList.add(new RefState("GA",32.9866,-83.6487));
    LookupList.add(new RefState("HI",21.1098,-157.5311));
    LookupList.add(new RefState("IA",42.0046,-93.2140));
    LookupList.add(new RefState("ID",44.2394,-114.5103));
    LookupList.add(new RefState("IL",40.3363,-89.0022));
    LookupList.add(new RefState("IN",39.8647,-86.2604));
    LookupList.add(new RefState("KS",38.5111,-96.8005));
    LookupList.add(new RefState("KY",37.6690,-84.6514));
    LookupList.add(new RefState("LA",31.1801,-91.8749));
    LookupList.add(new RefState("MA",42.2373,-71.5314));
    LookupList.add(new RefState("MD",39.0724,-76.7902));
    LookupList.add(new RefState("ME",44.6074,-69.3977));
    LookupList.add(new RefState("MI",43.3504,-84.5603));
    LookupList.add(new RefState("MN",45.7326,-93.9196));
    LookupList.add(new RefState("MO",38.4623,-92.3020));
    LookupList.add(new RefState("MP",14.8058,145.5505));
    LookupList.add(new RefState("MS",32.7673,-89.6812));
    LookupList.add(new RefState("MT",46.9048,-110.3261));
    LookupList.add(new RefState("NC",35.6411,-79.8431));
    LookupList.add(new RefState("ND",47.5362,-99.7930));
    LookupList.add(new RefState("NE",41.1289,-98.2883));
    LookupList.add(new RefState("NH",43.4108,-71.5653));
    LookupList.add(new RefState("NJ",40.3140,-74.5089));
    LookupList.add(new RefState("NM",34.8375,-106.2371));
    LookupList.add(new RefState("NV",38.4199,-117.1219));
    LookupList.add(new RefState("NY",42.1497,-74.9384));
    LookupList.add(new RefState("OH",40.3736,-82.7755));
    LookupList.add(new RefState("OK",35.5376,-96.9247));
    LookupList.add(new RefState("OR",44.5672,-122.1269));
    LookupList.add(new RefState("PA",40.5773,-77.2640));
    LookupList.add(new RefState("PR",18.2766,-66.3350));
    LookupList.add(new RefState("RI",41.6772,-71.5101));
    LookupList.add(new RefState("SC",33.8191,-80.9066));
    LookupList.add(new RefState("SD",44.2853,-99.4632));
    LookupList.add(new RefState("TN",35.7449,-86.7489));
    LookupList.add(new RefState("TX",31.1060,-97.6475));
    LookupList.add(new RefState("UT",40.1135,-111.8535));
    LookupList.add(new RefState("VA",37.7680,-78.2057));
    LookupList.add(new RefState("VI",18.0001,-64.8199));
    LookupList.add(new RefState("VT",44.0407,-72.7093));
    LookupList.add(new RefState("WA",47.3917,-121.5708));
    LookupList.add(new RefState("WI",44.2563,-89.6385));
    LookupList.add(new RefState("WV",38.4680,-80.9696));
    LookupList.add(new RefState("WY",42.7475,-107.2085));
  }

  public int find(Double coodLat, Double coodLong ) {

    latitude = coodLat;
    longitude = coodLong;

    return this.findNearestStation();
  }

  public int find(String coodLat, String coodLong ) {

    try {
      latitude = Double.parseDouble(coodLat);
    } catch (Exception e) {
      return -1;
    }

    try {
      longitude = Double.parseDouble(coodLong);
    } catch (Exception e) {
      return -1;
    }

    return this.findNearestStation();
  }

  public int findNearestStation() {
    int threshold =300000;
    for(int i = 0; i < LookupList.size(); ++i)
      if(this.findDistance(LookupList.get(i).Lat, LookupList.get(i).Long) <= threshold){
        this.country="US";
        this.state = LookupList.get(i).State;
        return 1;
      }
    return 0;
  }

  public Double findDistance (Double coodLat, Double coodLong) {

    // 'haversine' formula
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
