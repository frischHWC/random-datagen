package com.cloudera.frisch.randomdatagen;

import com.cloudera.frisch.randomdatagen.config.ArgumentsParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import com.cloudera.frisch.randomdatagen.parsers.Parser;
import com.cloudera.frisch.randomdatagen.sink.SinkInterface;
import com.cloudera.frisch.randomdatagen.sink.SinkSender;
import org.apache.log4j.Logger;

import java.util.List;


public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String [] args) {
        logger.info("Starting Application");
        long start = System.currentTimeMillis();

        logger.info("Getting arguments from command line to start the program");
        if(args.length < 4) {
            logger.error("There must be at least four arguments : model file path, number of rows per batch, number of batches, and sink to fill");
            System.exit(1);
        }
        ArgumentsParser.parseArgs(args);
        long numberOfBatches = (Long) ArgumentsParser.getArgsMap().get(ArgumentsParser.args.NUMBER_OF_BATCHES);
        long rowsPerBatch = (Long) ArgumentsParser.getArgsMap().get(ArgumentsParser.args.ROWS_PER_BATCH);

        logger.info("Parsing of model file");
        Parser parser = new JsonParser((String) ArgumentsParser.getArgsMap().get(ArgumentsParser.args.MODEL_FILE_PATH));
        Model model = parser.renderModelFromFile();

        logger.info("Initialization of all Sinks");
        List<SinkInterface> sinks = SinkSender.sinksInit(model);

        for(long i = 0; i < numberOfBatches; i++) {
            logger.info("Start to process batch " + (i+1) + "/" + numberOfBatches + " of " + rowsPerBatch + " rows");

            List<Row> randomDataList = model.generateRandomRows(rowsPerBatch);

            // Send Data to sinks in parallel if there are multiple sinks
            sinks.parallelStream().forEach(sink -> sink.sendOneBatchOfRows(randomDataList));

            // For tests only: print generated data
            if(logger.isDebugEnabled()) {
                randomDataList.forEach(data -> logger.debug("Data is : " + data.toString()));
            }

            logger.info("Finished to process batch " + (i+1) + "/" + numberOfBatches + " of " + rowsPerBatch + " rows");
        }

        // Terminate all sinks
        sinks.forEach(SinkInterface::terminate);

        // Recap of what has been generated
        Utils.recap(numberOfBatches, rowsPerBatch, (List<ArgumentsParser.sinks>) ArgumentsParser.getArgsMap().get(ArgumentsParser.args.SINK_TO_FILL), model);

        logger.info("Application Finished");
        logger.info("Application took : " + (System.currentTimeMillis()-start) + " ms to run");

    }

}
