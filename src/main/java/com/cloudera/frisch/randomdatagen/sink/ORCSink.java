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
import com.cloudera.frisch.randomdatagen.model.type.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * ORC File sink
 */
@SuppressWarnings("unchecked")
public class ORCSink implements SinkInterface {

    private final TypeDescription schema;
    private Writer writer;
    private final Map<String, ColumnVector> vectors;
    private final VectorizedRowBatch batch;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;


    /**
     * Init local ORC file
     */
    ORCSink(Model model) {
        this.counter = 0;
        this.model = model;
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.schema = model.getOrcSchema();
        this.batch = schema.createRowBatch();
        this.vectors = model.createOrcVectors(batch);

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName, "orc");
        }

        if (!oneFilePerIteration) {
            creatFileWithOverwrite(directoryName + fileName + ".orc");
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
        } catch (NullPointerException e) {
            logger.info("Writer was already closed");
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        if (oneFilePerIteration) {
            creatFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".orc");
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

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION)) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error(" Unable to close local file with error :", e);
            }
        }
    }

    private void creatFileWithOverwrite(String path) {
        try {
            Utils.deleteLocalFile(path);
            writer = OrcFile.createWriter(new Path(path),
                OrcFile.writerOptions(new Configuration())
                    .setSchema(schema));
        } catch (IOException e) {
            logger.warn("Could not create writer to ORC HDFS file due to error:", e);
        }
    }

}