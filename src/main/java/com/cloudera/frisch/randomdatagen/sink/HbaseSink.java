package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This is an HBase Sink using HBase API 2.3
 * It requires in config.properties to define zookeeper quorum, port, znode and type of authentication (simple or kerberos)
 * Each instance is only able to manage one connection to one specific table defined by property hbase.table.name in config.properties
 */
public class HbaseSink implements SinkInterface {

    private Connection connection;
    private Table table;

    public void init(Model model) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", PropertiesLoader.getProperty("hbase.zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", PropertiesLoader.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("zookeeper.znode.parent", PropertiesLoader.getProperty("zookeeper.znode.parent"));
        Utils.setupHadoopEnv(config);

        // Setup Kerberos auth if needed
        if (Boolean.valueOf(PropertiesLoader.getProperty("hbase.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hbase.security.user"),
                    PropertiesLoader.getProperty("hbase.security.keytab"), config);
            config.set("hbase.security.authentication", "kerberos");
        }

        try {
            connection = ConnectionFactory.createConnection(config);

            String fullTableName = model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE) + ":" +
                    model.getTableNames().get(OptionsConverter.TableNames.HBASE_TABLE_NAME);

            logger.warn("will create namespace");

            createNamespaceIfNotExists((String) model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE));

            if (!connection.getAdmin().tableExists(TableName.valueOf(fullTableName))) {

                HTableDescriptor tbdesc = new HTableDescriptor(TableName.valueOf(fullTableName));

                model.getHBaseColumnFamilyList().forEach(cf ->
                    tbdesc.addFamily(new HColumnDescriptor(Bytes.toBytes((String) cf)))
                );
                // In case of missing columns and to avoid troubles, a default 'cq' is created
                tbdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cq")));

                connection.getAdmin().createTable(tbdesc);
            }

            table = connection.getTable(TableName.valueOf(fullTableName));

        } catch (IOException e) {
            logger.error("Could not initiate HBase connection due to error: ", e);
            System.exit(1);
        }
    }

    public void terminate() {
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            logger.error("Impossible to close connection to HBase due to error: ", e);
            System.exit(1);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<com.cloudera.frisch.randomdatagen.model.Row> rows) {
        try {
            List<Put> putList = rows.parallelStream().map(com.cloudera.frisch.randomdatagen.model.Row::toHbasePut).collect(Collectors.toList());
            table.put(putList);
        } catch (Exception e) {
            logger.error("Could not write to HBase rows with error : ", e);
        }
    }

    private void createNamespaceIfNotExists(String namespace) {
        try {
            Admin admin = connection.getAdmin();
            try {
                admin.getNamespaceDescriptor(namespace);
            } catch (NamespaceNotFoundException e) {
                logger.debug("Namespace " + namespace + " does not exists, hence it will be created");
                admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            }
        } catch (IOException e) {
            logger.error("Could not create namespace " + namespace + " in Hbase, due to following error: ", e);
        }
    }


}
