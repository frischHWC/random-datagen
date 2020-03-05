package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * This is an ORC HDFS sink using Hadoop 3.2 API
 */
@SuppressWarnings("unchecked")
public class HdfsOrcSink implements SinkInterface {

    private FileSystem fileSystem;
    private TypeDescription schema;
    private Writer writer;
    private Map<? extends Field, ColumnVector> vectors;
    private VectorizedRowBatch batch;
    private int counter;
    private Model model;

    /**
     * Initiate HDFS connection with Kerberos or not
     *
     * @return filesystem connection to HDFS
     */
    public void init(Model model) {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.valueOf(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"), config);
        }

        logger.debug("Setting up access to ORC HDFS");
        try {
            fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to ORC HDFS !", e);
        }

        schema = model.getOrcSchema();
        batch = schema.createRowBatch();
        vectors = model.createOrcVectors(batch);

        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("false")) {
            String filepath = PropertiesLoader.getProperty("hdfs.uri") + model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + ".orc";

            deleteFile(filepath);

            try {
                writer = OrcFile.createWriter(new Path(filepath),
                        OrcFile.writerOptions(new Configuration())
                                .setSchema(schema));
            } catch (IOException e) {
                logger.warn("Could not create writer to ORC HDFS file due to error:", e);
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
            if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("false")) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close ORC HDFS file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            String filepath = PropertiesLoader.getProperty("hdfs.uri") + model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" + String.format("%010d", counter) + ".orc";

            deleteFile(filepath);

            try {
                writer = OrcFile.createWriter(new Path(filepath),
                        OrcFile.writerOptions(new Configuration())
                                .setSchema(schema));
            } catch (IOException e) {
                logger.warn("Could not create writer to ORC HDFS file due to error:", e);
            }
            counter++;
        }

        for (Row row : rows) {
            int rowNumber = batch.size++;
            row.fillinOrcVector(rowNumber, vectors);
            try {
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            } catch (IOException e) {
                logger.error("Can not write data to the ORC HDFS file due to error: ", e);
            }
        }

        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the ORC HDFS file due to error: ", e);
        }

        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error(" Unable to close ORC HDFS file with error :", e);
            }
        }


    }
}
