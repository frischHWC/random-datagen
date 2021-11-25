package com.cloudera.frisch.randomdatagen.sink;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is an HDFSJSON sink using Hadoop 3.1 API
 * Each instance manages one connection to a file system and one specific file
 */
public class HdfsJsonSink implements SinkInterface {

    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private int counter;
    private Model model;

    /**
     * Initiate HDFSJSON connection with Kerberos or not
     * @return filesystem connection to HDFSJSON
     */
    public void init(Model model) {
        Configuration config = new Configuration();
        Utils.setupHadoopEnv(config);

        // Set all kerberos if needed (Note that connection will require a user and its appropriate keytab with right privileges to access folders and files on HDFSCSV)
        if (Boolean.parseBoolean(PropertiesLoader.getProperty("hdfs.auth.kerberos"))) {
            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("hdfs.auth.kerberos.user"),
                    PropertiesLoader.getProperty("hdfs.auth.kerberos.keytab"),config);
        }

        logger.debug("Setting up access to HDFSJSON");
        try {
            fileSystem = FileSystem.get(URI.create(PropertiesLoader.getProperty("hdfs.uri")), config);
        } catch (IOException e) {
            logger.error("Could not access to HDFSJSON !", e);
        }

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllHdfsFiles(fileSystem, (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH),
                (String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME), "json");
        }

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + ".json");

        } else {
            createDirectory((String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH));
            counter = 0;
            this.model = model;
        }

    }

    void createFileWithOverwrite(String path) {
        try {
            fsDataOutputStream = fileSystem.create(new Path(path), true);
            logger.debug("Successfully created hdfs file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create file : " + path + " with no success :", e);
        }
    }

    void createDirectory(String path) {
        try {
            fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            logger.error("Unable to create directory of : " + path + " due to error: ", e);
        }
    }

    public void terminate() {
        try {
        fsDataOutputStream.close();
        } catch (IOException e) {
            logger.error(" Unable to close HDFSJSON file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows){
        try {
            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                        model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" + String.format("%010d", counter) + ".json");
                counter++;
            }

            List<String> rowsInString = rows.stream().map(Row::toJSON).collect(Collectors.toList());
            fsDataOutputStream.writeChars(String.join(System.getProperty("line.separator"), rowsInString));
            fsDataOutputStream.writeChars(System.getProperty("line.separator"));

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                fsDataOutputStream.close();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the HDFSJSON file due to error: ", e);
        }
    }

}
