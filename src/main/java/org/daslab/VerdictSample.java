package org.daslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.VerdictContext;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.jdbc41.VerdictConnection;
import scala.Tuple2;

/**
 * spark-submit --class org.daslab.VerdictSample xxx/xxx.jar "CREATE SCRAMBLE sample_table FROM origin_table RATIO 0.1"
 */
public class VerdictSample {
    /**
     * 本程序只能在集群上跑，猜测是因为hive的权限问题。
     * @param args
     */
    public static void main(String[] args) {
        //CREATE SCRAMBLE sample_table FROM origin_table RATIO 0.1
        String sql = args[0];

        SparkSession spark = SparkSession.builder().
            // 设置standalone模式下的master节点
            master("spark://10.176.24.40:7077").
//            master("local[*]").
            appName("VerdictSample").
            // 连接hive metastore
            config("hive.metastore.uris","thrift://10.176.24.40:9083").
            // 单个executor的核数
            config("spark.executor.cores","8").
            // 所有executor最大的核数
            config("spark.cores.max","32").
            // 单个executor的内存大小
            config("spark.executor.memory","16g").
            // 设置sparksql的数据仓库目录，可以不和hive相同
            config("spark.sql.warehouse.dir", "hdfs://10.176.24.40:9000/hive/warehouse").
            // 开启hive支持
            enableHiveSupport().
            getOrCreate();

        // 查看SparkConf所有的设置
        Tuple2<String, String>[] selfConfigs = spark.sparkContext().getConf().getAll();
        for (Tuple2<String, String> tuple2 : selfConfigs) {
            System.out.println(tuple2);
        }

        //为了解决bug：https://www.cnblogs.com/justinzhang/p/4983673.html
        Configuration configuration = spark.sparkContext().hadoopConfiguration();
        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

        //测试hive连接
        spark.sql("select * from bigbench_100g.websales_home_myshop limit 1").show();

        try {
            VerdictContext verdictContext = VerdictContext.fromSparkSession(spark);
            verdictContext.sql(sql);
        } catch (VerdictDBException e) {
            e.printStackTrace();
        }
    }
}
