import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

/**
 * Created by ruanzf on 2016/8/26.
 */
public class JedisUtil implements Serializable {

    private static volatile JedisPool jedisPool;

    public static Jedis getJedis() {
        if (null == jedisPool) {
            synchronized (JedisUtil.class) {
                if (null == jedisPool) {
                    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                    poolConfig.setMaxIdle(200);
                    poolConfig.setMaxWaitMillis(3000);
                    poolConfig.setMaxTotal(1000);
                    jedisPool = new JedisPool(poolConfig, "192.168.21.", 63);
                }
            }
        }
        return jedisPool.getResource();
    }
}
