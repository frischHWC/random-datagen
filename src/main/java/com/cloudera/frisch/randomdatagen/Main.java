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
package com.cloudera.frisch.randomdatagen;

import com.cloudera.frisch.randomdatagen.config.ArgumentsParser;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import com.cloudera.frisch.randomdatagen.parsers.Parser;
import com.cloudera.frisch.randomdatagen.sink.SinkInterface;
import com.cloudera.frisch.randomdatagen.sink.SinkSender;
import org.apache.log4j.Logger;

import java.util.List;

@SuppressWarnings("unchecked")
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

        int threads = 1;
        try {
            threads =
                Integer.parseInt(PropertiesLoader.getProperty("threads"));
        } catch (Exception e){
            logger.warn("Could nor load property of threads number, default to 1 thread");
        }
        logger.info("Will run generation using " + threads + " thread(s)");

        for(long i = 0; i < numberOfBatches; i++) {
            logger.info("Start to process batch " + (i+1) + "/" + numberOfBatches + " of " + rowsPerBatch + " rows");

            List<Row> randomDataList = model.generateRandomRows(rowsPerBatch, threads);

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

        // Compute and print time taken
        logger.info("Application Finished");
        logger.info("Application took : " + Utils.formatTimetaken(System.currentTimeMillis()-start) + " to run");

    }

}
