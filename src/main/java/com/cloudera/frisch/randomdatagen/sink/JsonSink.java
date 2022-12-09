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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a JSON sink
 * Its goal is to write into ONE single json file data randomly generated
 */
public class JsonSink implements SinkInterface {

    private FileOutputStream outputStream;
    private int counter;
    private final Model model;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;
    private final String lineSeparator;

    /**
     * Init local JSON file
     */
    public JsonSink(Model model) {
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);
        this.model = model;
        this.counter = 0;
        this.lineSeparator = System.getProperty("line.separator");

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "json");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".json");
        }
    }

    @Override
    public void terminate() {
        try {
            if (!oneFilePerIteration) {
                outputStream.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close local file with error :", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            if (oneFilePerIteration) {
                createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".json");
                counter++;
            }

            rows.stream().map(Row::toJSON).forEach(r -> {
                try {
                    outputStream.write(r.getBytes());
                    outputStream.write(lineSeparator.getBytes());
                } catch (IOException e) {
                    logger.error("Could not write row: " + r + " to file: " + outputStream.getChannel());
                }
            });
            outputStream.write(lineSeparator.getBytes());

            if (oneFilePerIteration) {
                outputStream.close();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the local file due to error: ", e);
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            File file = new File(path);
            if(file.createNewFile()) { logger.warn("Could not create file");}
            outputStream = new FileOutputStream(path, false);
            logger.debug("Successfully created local file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create file : " + path + " with no success :", e);
        }
    }


}
