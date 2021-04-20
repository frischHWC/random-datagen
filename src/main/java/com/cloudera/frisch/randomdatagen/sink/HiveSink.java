package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * This a HiveSink, each instance manages its own session and a preparedStatement for insertion
 * An inner class @see{com.cloudera.frisch.randomy.sink.HiveSinkParallel} below, allows multi threaded inserts,
 * It is recommended to not insert too many rows as Hive is very slow on insertion and use the batch function,
 * with a high number of rows per batch and few batches (to avoid recreating connection to Hive each time)
 * and with a maximum of 20 threads (configurable in config.properties)
 */
public class HiveSink implements SinkInterface {

    private static final int NUMBER_OF_THREADS = Integer.parseInt(PropertiesLoader.getProperty("hive.threads.number"));
    private Connection hiveConnection;
    private HdfsCsvSink hdfsSink;
    private String database;
    private String tableName;
    private String tableNameTemporary;
    private String insertStatement;
    private final boolean hiveOnHDFS = Boolean.parseBoolean(PropertiesLoader.getProperty("hive.on.hdfs"));

    public void init(Model model) {
        try {
            if (Boolean.parseBoolean(PropertiesLoader.getProperty("hive.auth.kerberos"))) {
                Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hive.security.user"),
                        PropertiesLoader.getProperty("hive.security.keytab"), new Configuration());
            }

            System.setProperty("javax.net.ssl.trustStore", PropertiesLoader.getProperty("hive.truststore.location"));
            System.setProperty("javax.net.ssl.trustStorePassword", PropertiesLoader.getProperty("hive.truststore.password"));

            java.util.Properties properties = new Properties();
            properties.put("tez.queue.name", "root.default");

            hiveConnection = DriverManager.getConnection("jdbc:hive2://" +
                            PropertiesLoader.getProperty("hive.zookeeper.server") + ":" +
                            PropertiesLoader.getProperty("hive.zookeeper.port") + "/" +
                            ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" +
                            PropertiesLoader.getProperty("hive.zookeeper.namespace") +
                            "?tez.queue.name=" + PropertiesLoader.getProperty("tez.queue.name")
                    , properties);

            database = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE);
            tableName = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME);
            tableNameTemporary = tableName + "_tmp";
            String locationTemporaryTable = (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH);

            prepareAndExecuteStatement("CREATE DATABASE IF NOT EXISTS " + database);

            hiveConnection.setSchema(database);

            logger.info("SQL schema for hive: " + model.getSQLSchema());
            prepareAndExecuteStatement("CREATE TABLE IF NOT EXISTS " + tableName + model.getSQLSchema());

            if (hiveOnHDFS) {
                // If using an HDFS sink, we want it to use the Hive HDFS File path and not the Hdfs file path
                hdfsSink = new HdfsCsvSink();
                model.getTableNames().put(OptionsConverter.TableNames.HDFS_FILE_PATH,
                        model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH));
                hdfsSink.init(model);

                logger.info("Creating temporary table");
                prepareAndExecuteStatement(
                        "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableNameTemporary + model.getSQLSchema() +
                                " LOCATION '" + locationTemporaryTable + "'" //+
                );
            }

            logger.info("SQL Insert schema for hive: " + model.getInsertSQLStatement());
            insertStatement = "INSERT INTO " + tableName + model.getInsertSQLStatement();

        } catch (SQLException e) {
            logger.error("Could not connect to HS2 and create table due to error: ", e);
        }
    }

    public void terminate() {
        try {
            if (hiveOnHDFS) {
                logger.info("Starting to load data to final table");
                prepareAndExecuteStatement("INSERT INTO " + tableName +
                        " SELECT * FROM " + tableNameTemporary);
            }

            hiveConnection.close();

        } catch (SQLException e) {
            logger.error("Could not close the Hive connection due to error: ", e);
        }
    }


    /**
     * As Hive insertions are very slow, this function has been designed to either send directly to Hive or use HDFSCSV
     *
     * @param rows list of rows to write to Hive
     */
    public void sendOneBatchOfRows(List<Row> rows) {
        if (hiveOnHDFS) {
            hdfsSink.sendOneBatchOfRows(rows);
        } else {
            senOneBatchOfRowsDirectlyToHive(rows);
        }
    }

    /**
     * As Hive insertions are very slow, this function has been designed to send request to Hive through many threads synchronously
     *
     * @param rows list of rows to write to Hive
     */
    private void senOneBatchOfRowsDirectlyToHive(List<Row> rows) {
        int lengthToTake = rows.size() / NUMBER_OF_THREADS;
        CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            HiveSinkParallel oneHiveSinkThread = new HiveSinkParallel(rows.subList(i * lengthToTake, i * lengthToTake + lengthToTake), latch);
            oneHiveSinkThread.setName("Thread-" + i);
            oneHiveSinkThread.start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted in the middle of a treatment : ", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * In order to accelerate Hive ingestion of data, this class has been created to allow parallelization of insertion
     * Hive is commit enabled by default (and cannot be changed), hence each request is taking few seconds to be fully committed and agreed by HIVE
     */
    private class HiveSinkParallel extends Thread {
        private List<Row> rows;
        private final CountDownLatch latch;

        HiveSinkParallel(List<Row> rows, CountDownLatch latch) {
            this.rows = rows;
            this.latch = latch;
        }

        @Override
        public void run() {
            try (HiveConnection hiveConnectionPerThread = new HiveConnection("jdbc:hive2://" +
                    PropertiesLoader.getProperty("hive.zookeeper.server") + ":" +
                    PropertiesLoader.getProperty("hive.zookeeper.port") + "/" +
                    database +
                    ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" +
                    PropertiesLoader.getProperty("hive.zookeeper.namespace")
                    , new Properties())) {

                HivePreparedStatement hivePreparedStatementByThread =
                        (HivePreparedStatement) hiveConnectionPerThread.prepareStatement(insertStatement);

                rows.forEach(row -> {
                    try {
                        logger.debug("Inserting one row " + row.toString() + " from thread : " + getName());

                        hivePreparedStatementByThread.clearParameters();
                        row.toHiveStatement(hivePreparedStatementByThread);
                        hivePreparedStatementByThread.execute();

                        logger.debug("Finished to insert one row " + row.toString() + " from thread : " + getName());
                    } catch (SQLException e) {
                        logger.error("Could not execute the request on row: " + row.toString() + " due to error: ", e);
                    } catch (StringIndexOutOfBoundsException e) {
                        logger.warn("This row has not been inserted due to a Hive API row bugs : " + row.toString());
                    }
                });

                hivePreparedStatementByThread.close();
            } catch (SQLException e) {
                logger.error("Could not prepare statement for Hive");
            }

            latch.countDown();
        }
    }

    private void prepareAndExecuteStatement(String sqlQuery) {
        try (PreparedStatement preparedStatement = hiveConnection.prepareStatement(sqlQuery)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            logger.error("Could not execute request due to error: ", e);
        }
    }


}
