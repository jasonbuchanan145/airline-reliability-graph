package edu.usd.hpc;

import lombok.Data;

import java.util.List;

@Data
public class Report {
    private List<Flight> leastDelayedDirect;
    private List<List<Flight>> leastDelayedOneHop;
    private long timeToCalculateDirectRoutes;
    private long timeToCalculateOneStopRoutes;
    private long totalTime;
    private int numberOfFlightsEvaluatedOneHop;
}
