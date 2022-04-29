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
 * This is a CSV sink to write to one or multiple CSV files locally
 *
 */
public class CSVSink implements SinkInterface {

    private FileOutputStream outputStream;
    private int counter;
    private final Model model;
    private final String lineSeparator;
    private final String directoryName;
    private final String fileName;
    private final Boolean oneFilePerIteration;

    /**
     * Init local CSV file with header
     */
    CSVSink(Model model) {
        this.model = model;
        this.counter = 0;
        this.lineSeparator = System.getProperty("line.separator");
        this.directoryName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH);
        this.fileName = (String) model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME);
        this.oneFilePerIteration = (Boolean) model.getOptionsOrDefault(OptionsConverter.Options.ONE_FILE_PER_ITERATION);

        Utils.createLocalDirectory(directoryName);

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            Utils.deleteAllLocalFiles(directoryName, fileName , "csv");
        }

        if (!oneFilePerIteration) {
            createFileWithOverwrite(directoryName + fileName + ".csv");
            appendCSVHeader(model);
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
                createFileWithOverwrite(directoryName + fileName + "-" + String.format("%010d", counter) + ".csv");
                appendCSVHeader(model);
                counter++;
            }

            rows.stream().map(Row::toCSV).forEach(r -> {
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

    void appendCSVHeader(Model model) {
        try {
            if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.CSV_HEADER)) {
                outputStream.write(model.getCsvHeader().getBytes());
                outputStream.write(lineSeparator.getBytes());
            }
        } catch (IOException e) {
            logger.error("Can not write header to the local file due to error: ", e);
        }
    }

    void createFileWithOverwrite(String path) {
        try {
            File file = new File(path);
            file.createNewFile();
            outputStream = new FileOutputStream(path, false);
            logger.debug("Successfully created local file : " + path);
        } catch (IOException e) {
            logger.error("Tried to create file : " + path + " with no success :", e);
        }
    }

}
