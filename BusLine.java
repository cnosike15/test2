package edu.virginia.sde.hw5;

public class BusLine {
    private int id;
    private boolean isActive;
    private String longName;
    private String shortName;
    private Route route;

    public BusLine(int id, boolean isActive, String longName, String shortName, Route route) {
        this.id = id;
        this.isActive = isActive;
        this.longName = longName;
        this.shortName = shortName;
        this.route = route;
    }

    public BusLine(int id, boolean isActive, String longName, String shortName) {
        this(id, isActive, longName, shortName, new Route());
    }

    public BusLine() { }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String getLongName() {
        return longName;
    }

    public void setLongName(String longName) {
        this.longName = longName;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public Route getRoute() {
        return route;
    }

    public void setRoute(Route route) {
        this.route = route;
    }

    public void addStopToRoute(Stop stop) {
        route.add(stop);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BusLine busLine = (BusLine) o;

        return id == busLine.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "BusLine{" +
                "id=" + id +
                ", isActive=" + isActive +
                ", longName='" + longName + '\'' +
                ", route=" + route +
                '}';
    }
}
