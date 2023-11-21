package edu.usd.hpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

@Service
public class SrcAndDest {
    @Autowired
    JdbcTemplate jdbcTemplate;

    List<String> getOrigins(){
        return jdbcTemplate.queryForList("select distinct origin from airlineAirportData order by origin",String.class);
    }
    List<String> getDestinations(){
        return jdbcTemplate.queryForList("select distinct dest from airlineAirportData order by dest",String.class);
    }
}
