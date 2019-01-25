package cn.lehome.dispatcher.utils.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/**
 * The type Action key topic service.
 *
 * @author zhuzz
 * @time 2018 /07/24 06:48:13
 */
public class ZookeeperUtils {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    public ZookeeperUtils() {
    }


    /**
     * Update zookeeper watcher.
     *
     * @param path the path
     * @param str  the str
     *
     * @author zhuzz
     * @time 2018 /09/13 14:55:41
     */
    public static void updateZookeeperWatcher(String zookeeperConnectStr, String path, String str) {

        try {
            ZooKeeper zk = new ZooKeeper(zookeeperConnectStr, 3000, null);
            Stat stat = zk.exists(path, null);
            byte[] data = Bytes.toBytes(str);
            if (Objects.isNull(stat)) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(path, data, stat.getVersion());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
