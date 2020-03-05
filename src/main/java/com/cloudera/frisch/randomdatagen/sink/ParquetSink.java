package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ParquetSink implements SinkInterface {


    private Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private int counter;
    private Model model;

    /**
     * Init local Parquet file
     */
    public void init(Model model) {

        schema = model.getAvroSchema();

        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("false")) {
            String filePath = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + ".parquet";

            deleteFile(filePath);

            try {
                writer = AvroParquetWriter
                        .<GenericRecord>builder(new Path(filePath))
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build();
            } catch (IOException e) {
                logger.warn("Could not create ParquetWriter", e);
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
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            String filePath = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" + String.format("%010d", counter) + ".parquet";

            deleteFile(filePath);

            try {
                writer = AvroParquetWriter
                        .<GenericRecord>builder(new Path(filePath))
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
                logger.error("Can not write data to the local file due to error: ", e);
            }
        });
        if (PropertiesLoader.getProperty("file.one.per.iteration").equalsIgnoreCase("true")) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error(" Unable to close local file with error :", e);
            }
        }
    }

}