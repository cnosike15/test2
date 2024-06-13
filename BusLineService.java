package edu.virginia.sde.hw5;

import java.sql.SQLException;
import java.util.*;

public class BusLineService {
    private final DatabaseDriver databaseDriver;

    public BusLineService(DatabaseDriver databaseDriver) {
        this.databaseDriver = databaseDriver;
    }

    public void addStops(List<Stop> stops) {
        try {
            databaseDriver.connect();
            databaseDriver.addStops(stops);
            databaseDriver.disconnect();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addBusLines(List<BusLine> busLines) {
        try {
            databaseDriver.connect();
            databaseDriver.addBusLines(busLines);
            databaseDriver.disconnect();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<BusLine> getBusLines() {
        try {
            databaseDriver.connect();
            var busLines = databaseDriver.getBusLines();
            databaseDriver.disconnect();
            return busLines;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Stop> getStops() {
        try {
            databaseDriver.connect();
            var stops = databaseDriver.getAllStops();
            databaseDriver.disconnect();
            return stops;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Route getRoute(BusLine busLine) {
        try {
            databaseDriver.connect();
            var stops = databaseDriver.getRouteForBusLine(busLine);
            databaseDriver.disconnect();
            return stops;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the closest stop to a given coordinate (using Euclidean distance, not great circle distance)
     * @param latitude - North/South coordinate (positive is North, Negative is South) in degrees
     * @param longitude - East/West coordinate (negative is West, Positive is East) in degrees
     * @return the closest Stop
     */
    public Stop getClosestStop(double latitude, double longitude) {
        try {
            databaseDriver.connect();
            Stop closest = null;
            double small_distance = Double.POSITIVE_INFINITY;
            for (Stop stop : databaseDriver.getAllStops()) {
                double stop_lat = stop.getLatitude();
                double stop_long = stop.getLongitude();
                if(Math.sqrt(Math.pow(latitude-stop_lat, 2)+Math.pow(longitude-stop_long, 2))<small_distance){
                    closest = stop;
                }
            }
            databaseDriver.disconnect();
            return closest;
        }
        catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Given two stop, a source and a destination, find the shortest (by distance) BusLine that starts
     * from source and ends at Destination.
     * @return Optional.empty() if no bus route visits both points
     * @throws IllegalArgumentException if either stop doesn't exist in the database
     */
    public Optional<BusLine> getRecommendedBusLine(Stop source, Stop destination) {
        try {
            Optional<BusLine> best_line = Optional.empty();
            double best_distance = Double.POSITIVE_INFINITY;
            databaseDriver.connect();
            if(!databaseDriver.getAllStops().containsAll(Arrays.asList(source, destination))){
                throw new IllegalArgumentException("stops not in database");
            }
            for(BusLine bus_line: databaseDriver.getBusLines()){
                Route bus_route = bus_line.getRoute();
                if(bus_route.contains(source)&&bus_route.contains(destination)){
                    double distance = bus_route.getRouteDistance();
                    if(distance<best_distance){
                        best_distance = distance;
                        best_line = Optional.of(bus_line);
                    }
                }
            }
            databaseDriver.disconnect();
            return best_line;
        }
        catch(SQLException e){
            throw new RuntimeException(e);
        }
    }
}
