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
    private Model model;

    /**
     * Initiate HDFS connection with Kerberos or not
     * @return filesystem connection to HDFS
     */
    public void init(Model model) {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"),config);
        }

        logger.debug("Setting up access to HDFS PARQUET");
        try {
            fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to HDFS PARQUET !", e);
        }

        schema = model.getAvroSchema();

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            String filepath = PropertiesLoader.getProperty("hdfs.uri") + model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + ".parquet";

            deleteFile(filepath);

            try {
                writer = AvroParquetWriter
                        .<GenericRecord>builder(new Path(filepath))
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_PAGE_SIZE))
                        .withDictionaryEncoding((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_ENCODING))
                        .withDictionaryPageSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_DICTIONARY_PAGE_SIZE))
                        .withRowGroupSize((int) model.getOptionsOrDefault(OptionsConverter.Options.PARQUET_ROW_GROUP_SIZE))
                        .build();
            } catch (IOException e) {
                logger.warn("Could not create ParquetWriter", e);
            }

        } else {
            counter = 0;
            this.model = model;
        }

    }

    void deleteFile(String path) {
        try {
            fileSystem.delete(new Path(path), true);
            logger.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create file : " + path + " with no success :", e);
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
        writer.close();
        } catch (IOException e) {
            logger.error(" Unable to close HDFS PARQUET file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows){
        try {
            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                String filepath =  PropertiesLoader.getProperty("hdfs.uri") + model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" + String.format("%010d", counter) + ".parquet";

                deleteFile(filepath);

                try {
                    writer = AvroParquetWriter
                            .<GenericRecord>builder(new Path(filepath))
                            .withSchema(schema)
                            .withConf(new Configuration())
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build();
                } catch (IOException e) {
                    logger.warn("Could not create ParquetWriter", e);
                }
                counter++;
            }

            rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
                try {
                    writer.write(genericRecord);
                } catch (IOException e) {
                    logger.error("Can not write data to the HDFS PARQUET file due to error: ", e);
                }
            });

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the HDFS PARQUET file due to error: ", e);
        }
    }

}
