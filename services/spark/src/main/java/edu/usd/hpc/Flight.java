package edu.usd.hpc;

import lombok.Data;

@Data
public class Flight {
    private String origin;
    private String originCityName;
    private String carrierName;
    private String dest;
    private String destCityName;
    private Double percentageDelayedLongerThan15;
    private Double percentageCancelled;
    private Double avgDelayLongerThan15;
    private int numFlights;

}
