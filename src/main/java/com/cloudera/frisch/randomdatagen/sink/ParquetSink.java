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
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
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


    private final Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;

    /**
     * Init local Parquet file
     */
    ParquetSink(Model model) {
        this.counter = 0;
        this.model = model;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);

        this.schema = model.getAvroSchema();

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "parquet");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".parquet");
        }

    }


    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close local file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if (oneFilePerIteration) {
            createFileWithOverwrite( directoryName + fileName + "-" + String.format("%010d", counter) + ".parquet");
            counter++;
        }
        rows.stream().map(row -> row.toGenericRecord(schema)).forEach(genericRecord -> {
            try {
                writer.write(genericRecord);
            } catch (IOException e) {
                logger.error("Can not write data to the local file due to error: ", e);
            }
        });
        if (oneFilePerIteration) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error(" Unable to close local file with error :", e);
            }
        }
    }

    private void createFileWithOverwrite(String path) {
        try {
            Utils.deleteLocalFile(path);
            this.writer = AvroParquetWriter
                    .<GenericRecord>builder(new Path(path))
                    .withSchema(schema)
                    .withConf(new Configuration())
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