package edu.virginia.sde.hw5;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouteTest {
    private Route emptyRoute, route;

    @Mock
    private Stop stopA, stopB, stopC;

    @Mock
    private List<Stop> stopList;

    @BeforeEach
    void setup() {
        emptyRoute = new Route();
    }

    @Test
    void isEmpty() {
        assertTrue(emptyRoute.isEmpty());
    }

    @Test
    void isEmpty_false() {
        route = new Route(new ArrayList<>(List.of(stopA)));
        assertFalse(route.isEmpty());
    }

    @Test
    void size_empty() {
        assertEquals(0, emptyRoute.size());
    }

    @Test
    void size_nonEmpty() {
        route = new Route(new ArrayList<>(List.of(stopA, stopB)));
        assertEquals(2, route.size());
    }

    @Test
    void get() {
        route = new Route(new ArrayList<>(List.of(stopA, stopB)));
        assertEquals(stopA, route.get(0));
    }

    @Test
    void add() {
        route = new Route(stopList);
        route.add(stopA);
        verify(stopList).add(stopA);
    }

    @Test
    void contains_true() {
        route = new Route(new ArrayList<>(List.of(stopA, stopB)));
        assertTrue(route.contains(stopB));
    }

    @Test
    void getStops() {
        route = new Route(new ArrayList<>(List.of(stopA, stopB)));
        var stopList = route.getStops();
        assertEquals(2, stopList.size());
        assertEquals(stopA, stopList.get(0));
        assertEquals(stopB, stopList.get(1));
    }

    @Test
    void getRouteDistance() {
        stopA = new Stop(1, "a", 0.0, 0.0);
        stopB = new Stop(1, "a", 3.0, 4.0); //a -> b = 5.0
        stopC = new Stop(1, "a", 4.0, 4.0); //b -> c = 6.0
        route = new Route(new ArrayList<>(List.of(stopA, stopB, stopC)));
        assertEquals(6.0, route.getRouteDistance());
    }
}