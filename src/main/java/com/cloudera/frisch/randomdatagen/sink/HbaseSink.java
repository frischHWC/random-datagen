/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
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
    private final String fullTableName;
    private final TableName tableName;
    private Admin admin;

    HbaseSink(Model model) {
        this.fullTableName = model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE) + ":" +
            model.getTableNames().get(OptionsConverter.TableNames.HBASE_TABLE_NAME);
        this.tableName = TableName.valueOf(fullTableName);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", PropertiesLoader.getProperty("hbase.zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", PropertiesLoader.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("zookeeper.znode.parent", PropertiesLoader.getProperty("zookeeper.znode.parent"));
        Utils.setupHadoopEnv(config);

        // Setup Kerberos auth if needed
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hbase.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hbase.security.user"),
                    PropertiesLoader.getProperty("hbase.security.keytab"), config);
            config.set("hbase.security.authentication", "kerberos");
        }

        try {
            this.connection = ConnectionFactory.createConnection(config);
            this.admin = connection.getAdmin();

            createNamespaceIfNotExists((String) model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE));

            if (admin.tableExists(tableName) && (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
                admin.deleteTable(tableName);
            }

            if (!admin.tableExists(tableName)) {

                TableDescriptorBuilder tbdesc = TableDescriptorBuilder.newBuilder(tableName);

                model.getHBaseColumnFamilyList().forEach(cf ->
                        tbdesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes((String) cf)).build())
                );
                // In case of missing columns and to avoid troubles, a default 'cq' is created
                tbdesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cq")).build());

                connection.getAdmin().createTable(tbdesc.build());
            }

            this.table = connection.getTable(tableName);

        } catch (IOException e) {
            logger.error("Could not initiate HBase connection due to error: ", e);
            System.exit(1);
        }
    }

    @Override
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
            List<Put> putList = rows.parallelStream()
                .map(com.cloudera.frisch.randomdatagen.model.Row::toHbasePut)
                .collect(Collectors.toList());
            table.put(putList);
        } catch (Exception e) {
            logger.error("Could not write to HBase rows with error : ", e);
        }
    }

    private void createNamespaceIfNotExists(String namespace) {
        try {
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
