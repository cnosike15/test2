package edu.virginia.sde.hw5;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class StopReader {

    private final URL busStopsApiUrl;

    public StopReader(Configuration configuration) {
        this.busStopsApiUrl = configuration.getBusStopsURL();
    }

    /**
     * Read all the stops from the "stops" json URL from Configuration Reader
     * @return List of stops
     */
    public List<Stop> getStops() {
        WebServiceReader webServiceReader = new WebServiceReader(busStopsApiUrl);
        JSONObject jsonRoot = webServiceReader.getJSONObject();
        List<Stop> stopList = new LinkedList<>();
        JSONArray stopArray = jsonRoot.getJSONArray("stops");
        for(Object busStop: stopArray){
            if(busStop instanceof JSONObject stops){
                var coordinates = stops.getJSONArray("position");
                stopList.add(new Stop(stops.getInt("id"), stops.getString("name"), coordinates.getDouble(0), coordinates.getDouble(1)));
            }
        }
        return stopList;
    }
}
