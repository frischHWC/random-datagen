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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * This is an HDFS Avro sink using Hadoop 3.2 API
 * Each instance manages one connection to a file system
 */
public class HdfsAvroSink implements SinkInterface {

    private final Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private final DatumWriter<GenericRecord> datumWriter;
    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final short replicationFactor;

    /**
     * Initiate HDFS-AVRO connection with Kerberos or not
     *
     */
    HdfsAvroSink(Model model) {
        this.counter = 0;
        this.model = model;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.replicationFactor = (short) model.getOptionsOrDefault(OptionsConverter.Options.HDFS_REPLICATION_FACTOR);

        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"), config);
        }

        try {
            this.fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to HDFSAVRO !", e);
        }

        this.schema = model.getAvroSchema();
        this.datumWriter = new GenericDatumWriter<>(schema);

        Utils.createHdfsDirectory(fileSystem, directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, directoryName, fileName, "avro");
        }

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
            createFileWithOverwrite(directoryName + fileName + ".avro");
            appendAvscHeader(model);
        }

    }

    @Override
    public void terminate() {
        try {
            dataFileWriter.close();
            fsDataOutputStream.close();
        } catch (IOException e) {
            logger.error(" Unable to close HDFSAVRO file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
            createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".avro");
            appendAvscHeader(model);
            counter++;
        }

        rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
            try {
                dataFileWriter.append(genericRecord);
            } catch (IOException e) {
                logger.error("Can not write data to the hdfs file due to error: ", e);
            }
        });

        if (oneFilePerIteration) {
            try {
                dataFileWriter.close();
                fsDataOutputStream.close();
            } catch (IOException e) {
                logger.error(" Unable to close hdfs file with error :", e);
            }
        } else {
            try {
                dataFileWriter.flush();
                fsDataOutputStream.flush();
            } catch (IOException e) {
                logger.error(" Unable to flush hdfs file with error :", e);
            }
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            Utils.deleteHdfsFile(fileSystem, path);
            fsDataOutputStream = fileSystem.create(new Path(path), replicationFactor);
            dataFileWriter = new DataFileWriter<>(datumWriter);
            logger.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create hdfs file : " + path + " with no success :", e);
        }
    }

    void appendAvscHeader(Model model) {
        try {
            dataFileWriter.create(schema, fsDataOutputStream.getWrappedStream());
        } catch (IOException e) {
            logger.error("Can not write header to the hdfs file due to error: ", e);
        }
    }

}
