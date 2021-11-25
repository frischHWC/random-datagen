package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * This is a kudu sink based on Kudu 1.11.0 API
 */
public class KuduSink implements SinkInterface {

    private KuduTable table;
    private KuduSession session;
    private KuduClient client;

    public void init(Model model) {
        try {

            System.setProperty("javax.net.ssl.trustStore", PropertiesLoader.getProperty("kudu.truststore.location"));
            System.setProperty("javax.net.ssl.trustStorePassword", PropertiesLoader.getProperty("kudu.truststore.password"));

            if (Boolean.valueOf(PropertiesLoader.getProperty("kudu.auth.kerberos"))) {
                Utils.loginUserWithKerberos(PropertiesLoader.getProperty("kudu.security.user"),
                        PropertiesLoader.getProperty("kudu.security.keytab"), new Configuration());

                UserGroupInformation.getLoginUser().doAs(
                        new PrivilegedExceptionAction<KuduClient>() {
                            @Override
                            public KuduClient run() throws Exception {
                                client = new KuduClient.KuduClientBuilder(PropertiesLoader.getProperty("kudu.master.server")).build();
                                return client;
                            }
                        });
            } else {
                client = new KuduClient.KuduClientBuilder(PropertiesLoader.getProperty("kudu.master.server")).build();
            }

            createTableIfNotExists((String) model.getTableNames().get(OptionsConverter.TableNames.KUDU_TABLE_NAME), model);

            session = client.newSession();

            switch ((String) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_REPLICAS)) {
            case "AUTO_FLUSH_SYNC": session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC); break;
            case "AUTO_FLUSH_BACKGROUND": session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND); break;
            case "MANUAL_FLUSH": session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); break;
            }
            session.setMutationBufferSpace((int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_BUFFER));

            table = client.openTable((String) model.getTableNames().get(OptionsConverter.TableNames.KUDU_TABLE_NAME));

        } catch (Exception e) {
            logger.error("Could not connect to Kudu due to error: ", e);
        }
    }


    private void createTableIfNotExists(String tableName, Model model) {
        /*
        try {
            client.deleteTable(tableName);
        }catch (KuduException e) {
            logger.warn("Could not delete table: " + tableName);
        }
        */

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas((int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_REPLICAS));

        if(!model.getKuduRangeKeys().isEmpty()) {
            cto.setRangePartitionColumns(model.getKuduRangeKeys());
        }
        if(!model.getKuduHashKeys().isEmpty()) {
            cto.addHashPartitions(model.getKuduHashKeys(), (int) model.getOptionsOrDefault(OptionsConverter.Options.KUDU_BUCKETS));
        }

        try {
            client.createTable(tableName, model.getKuduSchema(), cto);
        } catch (KuduException e) {
            if(e.getMessage().contains("already exists")){
                logger.info("Table Kudu : "  + tableName + " already exists, hence it will not be created");
            } else {
                logger.error("Could not create table due to error", e);
            }
        }

    }

    public void terminate() {
        try {
            session.close();
            client.shutdown();
        } catch (Exception e) {
            logger.error("Could not close connection to Kudu due to error: ", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            rows.parallelStream().map(row -> row.toKuduInsert(table)).forEach(insert -> {
                try {
                    session.apply(insert);
                } catch (KuduException e) {
                    logger.error("Could not insert row for kudu in table : " + table + " due to error:", e );
                }
            });
            session.flush();
        } catch (Exception e) {
            logger.error("Could not send rows to kudu due to error: ", e);
        }

    }
}
