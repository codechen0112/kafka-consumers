package org.sqkb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;



public class HbaseUtil {



    public static Configuration getConf()  {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "bigdata-002-112-129.sqkb-s.com:60000");
        configuration.set("hbase.zookeeper.quorum", "bigdata-001-112-130.sqkb-s.com,bigdata-002-112-129.sqkb-s.com,bigdata-003-112-128.sqkb-s.com,bigdata-004-112-131.sqkb-s.com,bigdata-005-112-127.sqkb-s.com");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return configuration;
    }

}
