package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;
import java.util.List;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = RedisSchema.getSlidingRateLimiterKey(name, windowSizeMS, maxHits);
        long currentTS = System.currentTimeMillis();
        try(Jedis jedis = jedisPool.getResource()){
            Transaction transaction = jedis.multi();
            transaction.zadd(key, currentTS, currentTS+"-"+Math.random());
            transaction.zremrangeByScore(key, Double.NEGATIVE_INFINITY, currentTS - windowSizeMS );
            Response<Long> hits = transaction.zcard(key);
            transaction.exec();
            if(hits.get() > maxHits){
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
