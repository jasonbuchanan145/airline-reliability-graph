package edu.usd.hpc;

import lombok.Data;

import java.util.List;

@Data
public class Report {
    private List<Flight> leastDelayedOneHop;
    private Flight leastCanceledOneHop;
    private List<List<Flight>> mostReliableTwoHop;
    private List<Flight> mostReliableThreeHop;

}
