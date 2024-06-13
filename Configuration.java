package edu.virginia.sde.hw5;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Objects;

public class Configuration {
    public static final String configurationFilename = "config.json";

    private URL busStopsURL;

    private URL busLinesURL;

    private String databaseFilename;

    public Configuration() { }

    public URL getBusStopsURL() {
        if (busStopsURL == null) {
            parseJsonConfigFile();
        }
        return busStopsURL;
    }

    public URL getBusLinesURL() {
        if (busLinesURL == null) {
            parseJsonConfigFile();
        }
        return busLinesURL;
    }

    public String getDatabaseFilename() {
        if (databaseFilename == null) {
            parseJsonConfigFile();
        }
        return databaseFilename;
    }

    /**
     * Parse the JSON file config.json to set all three of the fields:
     *  busStopsURL, busLinesURL, databaseFilename
     */
    private void parseJsonConfigFile() {
        try (InputStream inputStream = Objects.requireNonNull(Configuration.class.getResourceAsStream(configurationFilename));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            StringBuilder jsonAsString = new StringBuilder();
            String line = bufferedReader.readLine();
            while(line!=null){
                jsonAsString.append(line);
                line = bufferedReader.readLine();
            }
            JSONObject busInfo = new JSONObject(jsonAsString.toString());
            JSONObject endpoints = busInfo.getJSONObject("endpoints");
            busStopsURL = new URL(endpoints.getString("stops"));
            busLinesURL = new URL(endpoints.getString("lines"));
            databaseFilename = busInfo.getString("database");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
