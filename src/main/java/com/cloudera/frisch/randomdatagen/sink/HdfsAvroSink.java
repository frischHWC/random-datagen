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
 * This is an HDFSCSV sink using Hadoop 3.2 API
 * Each instance manages one connection to a file system and one specific file
 */
public class HdfsAvroSink implements SinkInterface {

    private Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private DatumWriter<GenericRecord> datumWriter;
    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private int counter;
    private Model model;

    /**
     * Initiate HDFSCSV connection with Kerberos or not
     *
     * @return filesystem connection to HDFSCSV
     */
    public void init(Model model) {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"), config);
        }

        logger.debug("Setting up access to HDFSAVRO");
        try {
            fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to HDFSAVRO !", e);
        }

        schema = model.getAvroSchema();

        datumWriter = new GenericDatumWriter<>(schema);

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + ".avro");

            appendAvscHeader(model);
        } else {
            counter = 0;
            this.model = model;
        }

    }

    void createFileWithOverwrite(String path) {
        try {
            fsDataOutputStream = fileSystem.create(new Path(path), true);
            dataFileWriter = new DataFileWriter<>(datumWriter);
            logger.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create hdfs file : " + path + " with no success :", e);
        }
    }

    void emptyDirectory(String path) {
        try {
            fileSystem.delete(new Path(path), true);
        } catch (IOException e) {
            logger.error("Unable to delete directory and subdirectories of : " + path + " due to error: ", e);
        }
    }

    public void terminate() {
        try {
            dataFileWriter.close();
            fsDataOutputStream.close();
        } catch (IOException e) {
            logger.error(" Unable to close HDFSAVRO file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" + String.format("%010d", counter) + ".avro");
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

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
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

    void appendAvscHeader(Model model) {
        try {
            dataFileWriter.create(schema, fsDataOutputStream.getWrappedStream());
        } catch (IOException e) {
            logger.error("Can not write header to the hdfs file due to error: ", e);
        }
    }

}
