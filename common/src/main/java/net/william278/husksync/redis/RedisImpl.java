package net.william278.husksync.redis;


import io.lettuce.core.RedisClient;
import net.william278.husksync.redis.redisdata.RedisAbstract;


public class RedisImpl extends RedisAbstract {

    public RedisImpl(RedisClient lettuceRedisClient) {
        super(lettuceRedisClient);
    }


}
