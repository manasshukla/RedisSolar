package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        // START Challenge #1
        Set<Site> sites = new HashSet<>();
        //Get the key of the set containing keys of all sites
        String setKey = RedisSchema.getSiteIDsKey();
        try(Jedis jedis = jedisPool.getResource()) {
            //Get all the member of the set
            Set<String> siteKeys= jedis.smembers(setKey);

            siteKeys.forEach(siteKey -> {
                //for each member in the set get the fields of the site from redis and convert it to a Site object
                Site site = new Site(jedis.hgetAll(siteKey));
                //Add the Site object to the Set of all sites.
                sites.add(site);
            });
        }
        return sites;
            // END Challenge #1
    }
}
