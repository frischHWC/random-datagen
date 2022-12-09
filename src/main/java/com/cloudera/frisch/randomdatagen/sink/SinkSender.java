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

import com.cloudera.frisch.randomdatagen.config.ArgumentsParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SinkSender {

    private SinkSender() { throw new IllegalStateException("Could not initialize this class");}

    private static final Logger logger = Logger.getLogger(SinkSender.class);

    /**
     * Check what are the all sinks passed and initiates them one by one
     * Each initialization of a sink belongs to it and prepares it to process
     * Moreover, it returns the list of all sinks once initialized
     * @return list of sinks initialized
     */
    @SuppressWarnings("unchecked")
    public static List<SinkInterface> sinksInit(Model model) {
        List<SinkInterface> sinkList = new ArrayList<>();
        List<ArgumentsParser.sinks> argsSinkList = Collections.emptyList();
        try {
            argsSinkList = (List<ArgumentsParser.sinks>) ArgumentsParser.getArgsMap().get(ArgumentsParser.args.SINK_TO_FILL);
        } catch (Exception e) {
            logger.warn("Could not cast sink list => Check parameters passed to the program ", e);
        }
        argsSinkList.forEach(sink -> {
            SinkInterface sinkToInitAndStart = null;
            switch (sink) {
                case HDFSCSV:
                    sinkToInitAndStart = new HdfsCsvSink(model);
                    break;
                case HDFSJSON:
                    sinkToInitAndStart = new HdfsJsonSink(model);
                    break;
                case HDFSAVRO:
                    sinkToInitAndStart = new HdfsAvroSink(model);
                    break;
                case HDFSORC:
                    sinkToInitAndStart = new HdfsOrcSink(model);
                    break;
                case HDFSPARQUET:
                    sinkToInitAndStart = new HdfsParquetSink(model);
                    break;
                case HBASE:
                    sinkToInitAndStart = new HbaseSink(model);
                    break;
                case HIVE:
                    sinkToInitAndStart = new HiveSink(model);
                    break;
                case OZONE:
                    sinkToInitAndStart = new OzoneSink(model);
                    break;
                case SOLR:
                    sinkToInitAndStart = new SolRSink(model);
                    break;
                case KAFKA:
                    sinkToInitAndStart = new KafkaSink(model);
                    break;
                case KUDU:
                    sinkToInitAndStart = new KuduSink(model);
                    break;
                case CSV:
                    sinkToInitAndStart = new CSVSink(model);
                    break;
                case JSON:
                    sinkToInitAndStart = new JsonSink(model);
                    break;
                case AVRO:
                    sinkToInitAndStart = new AvroSink(model);
                    break;
                case PARQUET:
                    sinkToInitAndStart = new ParquetSink(model);
                    break;
                case ORC:
                    sinkToInitAndStart = new ORCSink(model);
                    break;
                 default:
                     logger.warn("The sink " + sink + " provided has not been recognized as an expected sink");
                     break;
            }

            if(sinkToInitAndStart != null) {
                logger.info(sinkToInitAndStart.getClass().getSimpleName() + " is added to the list of sink");
                sinkList.add(sinkToInitAndStart);
            }

        });
        return sinkList;
    }

    /**
     * Check what are the all sinks passed and terminates them one by one
     * Each initialization of a sink belongs to it and prepares it to
     */
    public static void sinkTerminate(List<SinkInterface> sinks) {
       sinks.forEach(SinkInterface::terminate);
    }
}
