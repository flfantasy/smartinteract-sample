package org.daslab;

import org.apache.spark.sql.SparkSession;
import org.verdictdb.VerdictContext;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.jdbc41.VerdictConnection;

public class VerdictSample {
    public static void main(String[] args) {
        String sql = args[0];

        SparkSession spark = SparkSession.builder().
                master("spark://" + "ip_host").
                appName("VerdictSample").
                enableHiveSupport().
                config("spark.sql.warehouse.dir", "directory").
                getOrCreate();

        try {
            VerdictContext verdictContext = VerdictContext.fromSparkSession(spark);
            verdictContext.sql(sql);
        } catch (VerdictDBException e) {
            e.printStackTrace();
        }
    }
}
