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
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * This is an HDFS PARQUET sink using Hadoop 3.2 API
 */
public class HdfsParquetSink implements SinkInterface {

    private FileSystem fileSystem;
    private Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final short replicationFactor;
    private final Configuration conf;

    /**
     * Initiate HDFS connection with Kerberos or not
     * @return filesystem connection to HDFS
     */
    public HdfsParquetSink(Model model) {
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.model = model;
        this.counter = 0;
        this.replicationFactor = (short) model.getOptionsOrDefault(OptionsConverter.Options.HDFS_REPLICATION_FACTOR);
        this.conf = new Configuration();
        conf.set("dfs.replication", String.valueOf(replicationFactor));

        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"),config);
        }

        try {
            this.fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to HDFS PARQUET !", e);
        }

        this.schema = model.getAvroSchema();

        Utils.createHdfsDirectory(fileSystem, directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName, "parquet");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(PropertiesLoader.getProperty("hdfs.uri") + directoryName + fileName + ".parquet");
        }

    }


    @Override
    public void terminate() {
        try {
        writer.close();
        } catch (IOException e) {
            logger.error(" Unable to close HDFS PARQUET file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows){
        try {
            if (oneFilePerIteration) {
                createFileWithOverwrite(PropertiesLoader.getProperty("hdfs.uri") + directoryName + fileName + "-" + String.format("%010d", counter) + ".parquet");
                counter++;
            }

            rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
                try {
                    writer.write(genericRecord);
                } catch (IOException e) {
                    logger.error("Can not write data to the HDFS PARQUET file due to error: ", e);
                }
            });

            if (oneFilePerIteration) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the HDFS PARQUET file due to error: ", e);
        }
    }

    private void createFileWithOverwrite(String path) {
        try {
            Utils.deleteHdfsFile(fileSystem, path);
            this.writer = AvroParquetWriter
                .<GenericRecord>builder(new Path(path))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_PAGE_SIZE))
                .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
                .withDictionaryPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
                .withRowGroupSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
                .build();
            logger.debug("Successfully created local Parquet file : " + path);

        } catch (IOException e) {
            logger.error("Tried to create Parquet local file : " + path + " with no success :", e);
        }
    }

}
