package edu.usd.hpc;

import lombok.Data;

import java.util.List;

@Data
public class Report {
    private List<Flight> leastDelayedDirect;
    private List<Flight> leastCanceledDirect;
    private List<List<Flight>> leastDelayedOneHop;
    private long timeForInitializingTheGraph;
    private long timeToCalculateDirectRoutes;
    private long timeToCalculateOneStopRoutes;
    private long timeToPrepareTheReport;
    private int numberOfFlightsEvaluatedOneHop;
}
