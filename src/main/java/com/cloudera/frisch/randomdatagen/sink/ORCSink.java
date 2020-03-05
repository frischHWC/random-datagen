package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ORCSink implements SinkInterface {

    private TypeDescription schema;
    private Writer writer;
    private Map<? extends Field, ColumnVector> vectors;
    private VectorizedRowBatch batch;
    private int counter;
    private Model model;


    /**
     * Init local CSV file with header
     */
    public void init(Model model) {

        schema = model.getOrcSchema();
        batch = schema.createRowBatch();
        vectors = model.createOrcVectors(batch);

        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("false")) {
            String filepath = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + ".orc";

            deleteFile(filepath);

            try {
                writer = OrcFile.createWriter(new Path(filepath),
                        OrcFile.writerOptions(new Configuration())
                                .setSchema(schema));
            } catch (IOException e) {
                logger.warn("Could not create writer to ORC file due to error:", e);
            }
        } else {
            counter = 0;
            this.model = model;
        }
    }

    private void deleteFile(String path) {
        try {
            File fileTodelete = new File(path);
            if(fileTodelete.delete()) { logger.warn("Could not delete file");}
        } catch (Exception e) {
            logger.warn("Could not delete file : " + path + " due to error: ", e);
        }
    }

    public void terminate() {
        try {
            if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("false")) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close local file with error :", e);
        } catch (NullPointerException e) {
            logger.info("Writer was already closed");
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            String filepath = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" + String.format("%010d", counter) + ".orc";

            deleteFile(filepath);

            try {
                writer = OrcFile.createWriter(new Path(filepath),
                        OrcFile.writerOptions(new Configuration())
                                .setSchema(schema));
            } catch (IOException e) {
                logger.warn("Could not create writer to ORC file due to error:", e);
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
                logger.error("Can not write data to the local file due to error: ", e);
            }
        }

        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the local file due to error: ", e);
        }

        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error(" Unable to close local file with error :", e);
            }
        }
    }

}