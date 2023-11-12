package edu.usd.hpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Router {

    @Autowired
    private SparkRunner sparkRunner;
    @GetMapping("route")
    public Report route(@RequestParam String origin, @RequestParam String dest){
        return sparkRunner.report(origin, dest);
    }
}
