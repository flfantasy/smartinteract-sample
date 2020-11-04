package org.daslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.VerdictContext;
import org.verdictdb.exception.VerdictDBException;
import scala.Tuple2;

/**
 * 用sparksql在hive上执行一段sql，用于检查采样表和被采样表的条数，检验采样率是否正确
 */
public class SampleCheck {
    /**
     * 本程序只能在集群上跑，猜测是因为hive的权限问题。
     * @param args
     */
    public static void main(String[] args) {
        //"select count(*) from bigbench_10t.websales_home_myshop_10t"
        String sql = args[0];

        SparkSession spark = SparkSession.builder().
                // 设置standalone模式下的master节点
                master("spark://10.176.24.40:7077").
                appName("SampleCheck").
                // 连接hive metastore
                config("hive.metastore.uris","thrift://10.176.24.40:9083").
                // 单个executor的核数
                config("spark.executor.cores","8").
                // 所有executor最大的核数
                config("spark.cores.max","32").
                // 单个executor的内存大小
                config("spark.executor.memory","8g").
                // 设置sparksql的数据仓库目录，可以不和hive相同
                config("spark.sql.warehouse.dir", "hdfs://10.176.24.40:9000/hive/warehouse").
                // 开启hive支持
                enableHiveSupport().
                getOrCreate();


        //为了解决bug：https://www.cnblogs.com/justinzhang/p/4983673.html
        Configuration configuration = spark.sparkContext().hadoopConfiguration();
        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

        //测试hive连接
        spark.sql(sql).show();
//        spark.sql("select * from tpch.nation limit 10").show();

    }
}
