package edu.virginia.sde.hw5;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DatabaseDriver {
    private final String sqliteFilename;
    private Connection connection;

    public DatabaseDriver(Configuration configuration) {
        this.sqliteFilename = configuration.getDatabaseFilename();
    }

    public DatabaseDriver(String sqlListDatabaseFilename) {
        this.sqliteFilename = sqlListDatabaseFilename;
    }

    /**
     * Connect to a SQLite Database. This turns out Foreign Key enforcement, and disables auto-commits
     *
     * @throws SQLException
     */
    public void connect() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            throw new IllegalStateException("The connection is already opened");
        }
        connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteFilename);
        //the next line enables foreign key enforcement - do not delete/comment out
        connection.createStatement().execute("PRAGMA foreign_keys = ON");
        //the next line disables auto-commit - do not delete/comment out
        connection.setAutoCommit(false);
    }

    /**
     * Commit all changes since the connection was opened OR since the last commit/rollback
     */
    public void commit() throws SQLException {
        connection.commit();
    }

    /**
     * Rollback to the last commit, or when the connection was opened
     */
    public void rollback() throws SQLException {
        connection.rollback();
    }

    /**
     * Ends the connection to the database
     */
    public void disconnect() throws SQLException {
        connection.close();
    }

    /**
     * Creates the three database tables Stops, BusLines, and Routes, with the appropriate constraints including
     * foreign keys, if they do not exist already. If they already exist, this method does nothing.
     * As a hint, you'll need to create Routes last, and Routes must include Foreign Keys to Stops and
     * BusLines.
     *
     * @throws SQLException
     */
    public void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS Stops (" +
                    "ID INTEGER PRIMARY KEY, " +
                    "StopName TEXT NOT NULL, " +
                    "Latitude REAL NOT NULL, " +
                    "Longitude REAL NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS BusLines (" +
                    "ID INTEGER PRIMARY KEY, " +
                    "IsActive BOOLEAN NOT NULL, " +
                    "LongName TEXT NOT NULL, " +
                    "ShortName TEXT NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS Routes (" +
                    "ID INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "BusLineID INTEGER NOT NULL, " +
                    "StopID INTEGER NOT NULL, " +
                    "RouteOrder INTEGER NOT NULL, " +
                    "FOREIGN KEY (BusLineID) REFERENCES BusLines(ID) ON DELETE CASCADE, " +
                    "FOREIGN KEY (StopID) REFERENCES Stops(ID) ON DELETE CASCADE)");
        }
    }

    /**
     * Add a list of Stops to the Database. After adding all the stops, the changes will be committed. However,
     * if any SQLExceptions occur, this method will rollback and throw the exception.
     *
     * @param stops - the stops to be added to the database
     */
    public void addStops(List<Stop> stops) throws SQLException {
        String sql = "INSERT INTO Stops (ID, StopName, Latitude, Longitude) VALUES (?, ?, ?, ?)";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            for (Stop stop : stops) {
                prepared_statement.setInt(1, stop.getId());
                prepared_statement.setString(2, stop.getName());
                prepared_statement.setDouble(3, stop.getLatitude());
                prepared_statement.setDouble(4, stop.getLongitude());
                prepared_statement.executeUpdate();
            }
        } catch (SQLException e) {
            rollback();
            throw e;
        }
    }

    /**
     * Gets a list of all Stops in the database
     */
    public List<Stop> getAllStops() throws SQLException {
        List<Stop> stops = new ArrayList<>();
        String sql = "SELECT ID, StopName, Latitude, Longitude FROM Stops";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql);
             ResultSet resultset = prepared_statement.executeQuery()) {
            while (resultset.next()) {
                stops.add(new Stop(resultset.getInt("ID"), resultset.getString("StopName"),
                        resultset.getDouble("Latitude"), resultset.getDouble("Longitude")));
            }
        }
        return stops;
    }

    /**
     * Get a Stop by its ID number. Returns Optional.isEmpty() if no Stop matches the ID.
     */
    public Optional<Stop> getStopById(int stopId) throws SQLException {
        String sql = "SELECT ID, StopName, Latitude, Longitude FROM Stops WHERE ID = ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setInt(1, stopId);
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                if (resultset.next()) {
                    Stop stop = new Stop(
                            resultset.getInt("ID"),
                            resultset.getString("StopName"),
                            resultset.getDouble("Latitude"),
                            resultset.getDouble("Longitude")
                    );
                    return Optional.of(stop);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Get all Stops whose name contains the substring (case-insensitive). For example, the parameter "Rice"
     * would return a List of Stops containing "Whitehead Rd @ Rice Hall"
     */
    public List<Stop> getStopsByName(String subString) throws SQLException {
        List<Stop> stops = new ArrayList<>();
        String sql = "SELECT * FROM Stops WHERE StopName LIKE ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setString(1, "%" + subString + "%");
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                while (resultset.next()) {
                    stops.add(new Stop(resultset.getInt("ID"), resultset.getString("StopName"),
                            resultset.getDouble("Latitude"), resultset.getDouble("Longitude")));
                }
            }
        }
        return stops;
    }

    /**
     * Add BusLines and their Routes to the database, including their routes. This method should only be called after
     * Stops are added to the database via addStops, since Routes depends on the StopIds already being
     * in the database. If any SQLExceptions occur, this method will rollback all changes since
     * the method was called. This could happen if, for example, a BusLine contains a Stop that is not in the database.
     */
    public void addBusLines(List<BusLine> busLines) throws SQLException {
        String insertBusLineSQL = "INSERT INTO BusLines (ID, IsActive, LongName, ShortName) VALUES (?, ?, ?, ?)";
        String insertRouteSQL = "INSERT INTO Routes (BusLineID, StopID, RouteOrder) VALUES (?, ?, ?)";

        try (PreparedStatement preparedStatementBusLine = connection.prepareStatement(insertBusLineSQL);
             PreparedStatement preparedstatementRoute = connection.prepareStatement(insertRouteSQL)) {

            for (BusLine busLine : busLines) {
                preparedStatementBusLine.setInt(1, busLine.getId());
                preparedStatementBusLine.setBoolean(2, busLine.isActive());
                preparedStatementBusLine.setString(3, busLine.getLongName());
                preparedStatementBusLine.setString(4, busLine.getShortName());
                preparedStatementBusLine.executeUpdate();

                int order = 0;
                for (Stop stop : busLine.getRoute().getStops()) {
                    preparedstatementRoute.setInt(1, busLine.getId());
                    preparedstatementRoute.setInt(2, stop.getId());
                    preparedstatementRoute.setInt(3, order++);
                    preparedstatementRoute.executeUpdate();
                }
            }
        } catch (SQLException e) {
            rollback();
            throw e;
        }
    }

    /**
     * Return a list of all BusLines
     */
    public List<BusLine> getBusLines() {
        List<BusLine> busLines = new ArrayList<>();
        String sql = "SELECT ID, IsActive, LongName, ShortName FROM BusLines";

        try (PreparedStatement prepared_statement = connection.prepareStatement(sql);
             ResultSet resultset = prepared_statement.executeQuery()) {
            while (resultset.next()) {
                BusLine busLine = new BusLine(
                        resultset.getInt("ID"),
                        resultset.getBoolean("IsActive"),
                        resultset.getString("LongName"),
                        resultset.getString("ShortName")
                );
                busLines.add(busLine);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error accessing database", e);
        }
        return busLines;
    }

    /**
     * Get a BusLine by its id number. Return Optional.empty() if no busLine is found
     */
    public Optional<BusLine> getBusLinesById(int busLineId) throws SQLException {
        String sql = "SELECT * FROM BusLines WHERE ID = ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setInt(1, busLineId);
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                if (resultset.next()) {
                    return Optional.of(new BusLine(resultset.getInt("ID"),
                            resultset.getBoolean("IsActive"),
                            resultset.getString("LongName"),
                            resultset.getString("ShortName")));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Get BusLine by its full long name (case-insensitive). Return Optional.empty() if no busLine is found.
     */
    public Optional<BusLine> getBusLineByLongName(String longName) throws SQLException {
        String sql = "SELECT * FROM BusLines WHERE LongName LIKE ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setString(1, longName);
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                if (resultset.next()) {
                    return Optional.of(new BusLine(resultset.getInt("ID"),
                            resultset.getBoolean("IsActive"),
                            resultset.getString("LongName"),
                            resultset.getString("ShortName")));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Get BusLine by its full short name (case-insensitive). Return Optional.empty() if no busLine is found.
     */
    public Optional<BusLine> getBusLineByShortName(String shortName) throws SQLException {
        String sql = "SELECT * FROM BusLines WHERE ShortName LIKE ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setString(1, shortName);
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                if (resultset.next()) {
                    return Optional.of(new BusLine(resultset.getInt("ID"),
                            resultset.getBoolean("IsActive"),
                            resultset.getString("LongName"),
                            resultset.getString("ShortName")));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Get all BusLines that visit a particular stop
     */
    public List<BusLine> getBusLinesByStop(Stop stop) throws SQLException {
        List<BusLine> busLines = new ArrayList<>();
        String sql = "SELECT DISTINCT b.ID, b.IsActive, b.LongName, b.ShortName FROM BusLines b " +
                "JOIN Routes r ON b.ID = r.BusLineID WHERE r.StopID = ?";
        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setInt(1, stop.getId());
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                while (resultset.next()) {
                    busLines.add(new BusLine(resultset.getInt("ID"),
                            resultset.getBoolean("IsActive"),
                            resultset.getString("LongName"),
                            resultset.getString("ShortName")));
                }
            }
        }
        return busLines;
    }

    /**
     * Returns a BusLine's route as a List of stops *in-order*
     *
     * @param busLine
     * @throws SQLException
     * @throws java.util.NoSuchElementException if busLine is not in the database
     */
    public Route getRouteForBusLine(BusLine busLine) throws SQLException {
        List<Stop> stops = new ArrayList<>();
        String sql = "SELECT s.ID, s.StopName, s.Latitude, s.Longitude FROM Stops s "
                + "JOIN Routes r ON s.ID = r.StopID WHERE r.BusLineID = ? ORDER BY r.RouteOrder";

        try (PreparedStatement prepared_statement = connection.prepareStatement(sql)) {
            prepared_statement.setInt(1, busLine.getId());
            try (ResultSet resultset = prepared_statement.executeQuery()) {
                while (resultset.next()) {
                    Stop stop = new Stop(
                            resultset.getInt("ID"),
                            resultset.getString("StopName"),
                            resultset.getDouble("Latitude"),
                            resultset.getDouble("Longitude")
                    );
                    stops.add(stop);
                }
            }
        }
        return new Route(stops);
    }

    /**
     * Removes all data from the tables, leaving the tables empty (but still existing!). As a hint, delete the
     * contents of Routes firesultsett in order to avoid violating foreign key constraints.
     */
    public void clearTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM Routes");
            stmt.execute("DELETE FROM BusLines");
            stmt.execute("DELETE FROM Stops");
        }
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        DatabaseDriver databaseDriver = new DatabaseDriver(config);
        StopReader stopReader = new StopReader(config);
        BusLineReader busLineReader = new BusLineReader(config);

        try {
            databaseDriver.connect();
            databaseDriver.createTables();

            // add stops from API
            List<Stop> stopsFromAPI = stopReader.getStops();
            databaseDriver.addStops(stopsFromAPI);
            System.out.println("Added stops from API.");



            // tries adding stop back
            Stop stopToAddBack = stopsFromAPI.get(0);
            databaseDriver.addStops(Collections.singletonList(stopToAddBack));
            System.out.println("Added back stop ID " + stopToAddBack.getId());

            // adds & removes busline
            List<BusLine> busLinesFromAPI = busLineReader.getBusLines();
            databaseDriver.addBusLines(busLinesFromAPI);
            System.out.println("Added bus lines from API.");

            // duplicate stops
            try {
                databaseDriver.addStops(Collections.singletonList(stopToAddBack)); // Should throw if duplicates not allowed
                System.out.println("Attempted to add duplicate stop. Should not see this if constraints work.");
            } catch (SQLException e) {
                System.out.println("Expected failure on adding duplicate stop.");
            }

            // closest stop
            double refLat = 38.0293;
            double refLon = -78.4767;
            Stop closestStop = null;
            double minDistance = Double.MAX_VALUE;
            for (Stop stop : stopsFromAPI) {
                double distance = Math.sqrt(Math.pow(stop.getLatitude() - refLat, 2) + Math.pow(stop.getLongitude() - refLon, 2));
                if (distance < minDistance) {
                    closestStop = stop;
                    minDistance = distance;
                }
            }
            System.out.println("Closest stop is: " + closestStop.getName() + " with ID: " + closestStop.getId());


        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (databaseDriver != null) {
                    databaseDriver.disconnect();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}