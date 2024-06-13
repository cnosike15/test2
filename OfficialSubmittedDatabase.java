package edu.virginia.sde.hw5;

import java.sql.SQLException;
import java.util.List;

public class OfficialSubmittedDatabase {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        DatabaseDriver databaseDriver = new DatabaseDriver(configuration);
        BusLineReader readBusLine = new BusLineReader(configuration);
        StopReader readBusStop = new StopReader(configuration);
        Route routeReader = new Route();
        double routes = routeReader.getRouteDistance();

        try {
            databaseDriver.connect();
            List<Stop> stops = readBusStop.getStops();
            List <BusLine> buslines = readBusLine.getBusLines();
            databaseDriver.clearTables();
            databaseDriver.createTables();
            databaseDriver.addStops(stops);
            databaseDriver.addBusLines(buslines);
//            databaseDriver.getRouteForBusLine(routeReader);
            databaseDriver.commit();



        } catch (SQLException e) {
//            throw new RuntimeException(e);
            System.out.println("Error adding values to the database" + e.getMessage());
        }
        try {
            databaseDriver.rollback();
        }
        catch (SQLException exe) {
            System.out.println("Error rolling back" + exe.getMessage());
        }
        finally {
            try {
                databaseDriver.disconnect();
            }
            catch (SQLException e) {
                System.out.println("Error disconnecting" + e.getMessage());
            }
        }

    }


}
