package org.daslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.VerdictContext;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.jdbc41.VerdictConnection;
import scala.Tuple2;

/**
 * 1、调用命令：
 * spark-submit --class org.daslab.VerdictSample xxx/xxx.jar "CREATE SCRAMBLE sample_table FROM origin_table RATIO 0.1"
 * 由于一些未知的原因，可能导致新表未创建，但是hdfs中有新表数据；
 * 可以根据具有相同表结构的表的创建语句（show create xxx），创建新表；
 * 再在hdfs中将新表数据复制到新表目录下，再在hive中使用MSCK REPAIR TABLE XXX修复数据。
 *
 * 2、采样后的表在hdfs里parquet小文件太多，拖累后续的计算速度，需要整合，使用如下hql：
 * create table new_table as select * from origin_table
 * 但是当小文件过于多时，该条语句执行时会产生大量map task和reduce task，创建了大量临时文件，超过了hive中配置的限制。
 * 所以需要用parquet-tools来手动合并parquet文件。
 * 先clone apache/parquet-mr的git项目，直接编译parquet-tools子项目（一定要在终端完成，记得使用-Plocal）
 * 然后从hdfs上把parquet文件复制到linux，使用
 * java -jar parquet-tools-1.12.0-SNAPSHOT.jar merge /input/ /output_file
 * 再把合并后的parquet文件传到hdfs中对应目录，使用1中的方法建立成新表，再使用
 * CREATE TABLE XXX AS SELECT * FROM YYY
 * 真正合并为一个紧凑的表。
 */
public class VerdictSample {
    /**
     * 本程序只能在集群上跑，猜测是因为hive的权限问题。
     * @param args
     */
    public static void main(String[] args) {
        //"CREATE SCRAMBLE sample_table FROM origin_table RATIO 0.1"
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
