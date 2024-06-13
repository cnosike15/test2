package edu.virginia.sde.hw5;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BusLineReader {
    private final URL busLinesApiUrl;
    private final URL busStopsApiUrl;

    /* You'll need this to get the Stop objects when building the Routes object */
    private final StopReader stopReader;
    /**
     * Returns a list of BusLine objects. This is a "deep" list, meaning all the BusLine objects
     * already have their Route objects fully populated with that line's Stops.
     */

    public BusLineReader(Configuration configuration) {
        this.busStopsApiUrl = configuration.getBusStopsURL();
        this.busLinesApiUrl = configuration.getBusLinesURL();
        stopReader = new StopReader(configuration);
    }

    /**
     * This method returns the BusLines from the API service, including their
     * complete Routes.
     */
    public List<BusLine> getBusLines() {
        WebServiceReader webLineServiceReader = new WebServiceReader(busLinesApiUrl);
        JSONObject jsonLineRoot = webLineServiceReader.getJSONObject();
        WebServiceReader webStopServiceReader = new WebServiceReader(busStopsApiUrl);
        JSONObject jsonStopRoot = webStopServiceReader.getJSONObject();
        List<BusLine> busList = new ArrayList<>();
        JSONArray lineArray = jsonLineRoot.getJSONArray("lines");
        List<Stop> stopList = stopReader.getStops();
        JSONArray routeArray = jsonStopRoot.getJSONArray("routes");
        for(Object busLine : lineArray){
            if(busLine instanceof JSONObject lines){
                int id = lines.getInt("id");
                boolean isActive = lines.getBoolean("is_active");
                String long_name = lines.getString("long_name");
                String short_name = lines.getString("short_name");
                Route busRoute = new Route();
                for(Object route: routeArray){
                    if(route instanceof JSONObject routes){
                        if(id == routes.getInt("id")){
                            JSONArray stopArray = routes.getJSONArray("stops");
                            for(Object stopPoint: stopArray){
                                for(Stop s: stopList){
                                    if(s.getId()==(int)stopPoint){
                                        busRoute.add(s);
                                        break;
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
                busList.add(new BusLine(id, isActive, long_name, short_name, busRoute));
            }
        }

        return busList;
    }
}
