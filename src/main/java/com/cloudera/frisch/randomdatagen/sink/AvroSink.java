package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class AvroSink implements SinkInterface {

    private File file;
    private Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private DatumWriter<GenericRecord> datumWriter;
    private int counter;
    private Model model;

    /**
     * Init local Avro file with header
     */
    public void init(Model model) {

        schema = model.getAvroSchema();

        datumWriter = new GenericDatumWriter<>(schema);

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {

            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + ".avro");

            appendAvscHeader(model);

        } else {
            this.model = model;
            counter = 0;
        }

    }

    void createFileWithOverwrite(String path) {
        try {
            file = new File(path);
            if(!file.createNewFile()) { logger.warn("Could not create file");}
            dataFileWriter = new DataFileWriter<>(datumWriter);
            logger.debug("Successfully created local file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create file : " + path + " with no success :", e);
        }
    }

    public void terminate() {
        try {
            if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                dataFileWriter.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close local file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" + String.format("%010d", counter) + ".avro");

            appendAvscHeader(model);
            counter++;
        }

        rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
            try {
                dataFileWriter.append(genericRecord);
            } catch (IOException e) {
                logger.error("Can not write data to the local file due to error: ", e);
            }
        });

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                logger.error(" Unable to close local file with error :", e);
            }
        } else {
            try {
                dataFileWriter.flush();
            } catch (IOException e) {
                logger.error(" Unable to flush local file with error :", e);
            }
        }
    }

    void appendAvscHeader(Model model) {
        try {
            dataFileWriter.create(schema, file);
        } catch (IOException e) {
            logger.error("Can not write header to the local file due to error: ", e);
        }
    }
}
