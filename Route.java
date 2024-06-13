package edu.virginia.sde.hw5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Route implements Iterable<Stop> {
    private final List<Stop> stopList;

    public Route(List<Stop> stopList) {
        this.stopList = stopList;
    }

    public Route() {
        this(new ArrayList<>());
    }

    public boolean isEmpty() {
        return stopList.isEmpty();
    }

    public int size() {
        return stopList.size();
    }

    public Stop get(int index) {
        return stopList.get(index);
    }

    public void add(Stop newStop) {
        stopList.add(newStop);
    }

    public boolean contains(Stop newStop) {
        return stopList.contains(newStop);
    }

    public List<Stop> getStops() {
        return Collections.unmodifiableList(stopList);
    }

    public double getRouteDistance() {
        double totalDistance = 0.0;
        for (int i = 0; i < size() - 1; i++) {
            Stop segmentStart = get(i);
            Stop segmentEnd = get(i+1);
            double segmentDistance = segmentStart.distanceTo(segmentEnd);
            totalDistance += segmentDistance;
        }
        return totalDistance;
    }

    @Override
    public Iterator<Stop> iterator() {
        return stopList.iterator();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Route stops = (Route) o;

        return stopList.equals(stops.stopList);
    }

    @Override
    public int hashCode() {
        return stopList.hashCode();
    }

    @Override
    public String toString() {
        return "Route{" +
                "stopList=" + stopList +
                '}';
    }
}
