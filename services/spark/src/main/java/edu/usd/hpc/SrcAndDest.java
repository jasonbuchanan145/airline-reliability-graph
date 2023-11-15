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
    //get the origins from  the db. Cache the response in memory so we don't have to query everytime
    @Cacheable("origins")
    List<String> getOrigins(){
        return jdbcTemplate.queryForList("select distinct origin from airlineAirportData",String.class);
    }
    @Cacheable("destination")
    List<String> getDestinations(){
        return jdbcTemplate.queryForList("select distinct dest from airlineAirportData",String.class);
    }
}
