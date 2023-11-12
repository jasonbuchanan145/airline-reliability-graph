package edu.usd.hpc;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
public class Flight {
    private int id;
    private String origin;
    private String originCityName;
    private String originState;
    private String carrierName;
    private String dest;
    private String destCityName;
    private String destState;
    private Double percentageDelayed;
    private Double percentageDelayedLongerThan15;
    private Double percentage_cancelled;
    private Double avg_delay;
    private Double avgDelayLongerThan15;
    private int numFlights;

}
