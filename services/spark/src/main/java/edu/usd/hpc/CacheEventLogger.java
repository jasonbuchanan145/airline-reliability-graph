package edu.usd.hpc;

import lombok.extern.slf4j.Slf4j;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

@Slf4j
public class CacheEventLogger implements CacheEventListener<Object, Object> {
    // mostly copy paste from
    //https://www.baeldung.com/spring-boot-ehcache. Just needed
    //something to log the cache was actually written to and accessed

    @Override
    public void onEvent(
            CacheEvent<?, ?> cacheEvent) {
        log.info("Cache event %s %s type", cacheEvent.getKey(), cacheEvent.getType());
    }
}