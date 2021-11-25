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
    private Model model;

    /**
     * Init local JSON file
     */
    public void init(Model model) {

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles((String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH),
                (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) , "json");
        }

        if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
            createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + ".json");
        } else {
            counter = 0;
            this.model = model;
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

    public void terminate() {
        try {
            if (!(Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                outputStream.close();
            }
        } catch (IOException e) {
            logger.error(" Unable to close local file with error :", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                createFileWithOverwrite((String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                        model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" + String.format("%010d", counter) + ".json");
                counter++;
            }

            List<String> rowsInString = rows.stream().map(Row::toJSON).collect(Collectors.toList());
            outputStream.write(String.join(System.getProperty("line.separator"), rowsInString).getBytes());
            outputStream.write(System.getProperty("line.separator").getBytes());

            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION)) {
                outputStream.close();
            }
        } catch (IOException e) {
            logger.error("Can not write data to the local file due to error: ", e);
        }
    }


}
