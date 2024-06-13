package edu.virginia.sde.hw5;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class WebServiceReader {
    private final URL url;

    public WebServiceReader(URL url) {
        this.url = url;
    }

    /**
     * Extract the JSONObject from the provided URL;
     * @return
     */
    public JSONObject getJSONObject() {
        try(var inputStream = url.openStream();
            var inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            var bufferedReader = new BufferedReader(inputStreamReader)) {
            var fileContents = bufferedReader.lines().collect(Collectors.joining("\n"));
            return new JSONObject(fileContents);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
