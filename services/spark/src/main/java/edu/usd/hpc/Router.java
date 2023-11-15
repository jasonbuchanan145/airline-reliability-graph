package edu.usd.hpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
@RestController
public class Router {

    @Autowired
    private SparkRunner sparkRunner;


    @Autowired
    private SrcAndDest srcAndDest;

    @GetMapping("route")
    public Report route(@RequestParam String origin, @RequestParam String dest){
        return sparkRunner.report(origin, dest);
    }
    @GetMapping("origins")
    public List<String> origins(){
        return srcAndDest.getOrigins();
    }
    @GetMapping("destinations")
    public List<String> destination(){
        return srcAndDest.getDestinations();
    }

}
