package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


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
    public static List<SinkInterface> sinksInit(Model model, Map<ApplicationConfigs, String> properties, List<SinkParser.sinks> sinks) {
        List<SinkInterface> sinkList = new ArrayList<>();

        sinks.forEach(sink -> {
            SinkInterface sinkToInitAndStart = null;
            switch (sink) {
                case HDFSCSV:
                    sinkToInitAndStart = new HdfsCsvSink(model, properties);
                    break;
                case HDFSJSON:
                    sinkToInitAndStart = new HdfsJsonSink(model, properties);
                    break;
                case HDFSAVRO:
                    sinkToInitAndStart = new HdfsAvroSink(model, properties);
                    break;
                case HDFSORC:
                    sinkToInitAndStart = new HdfsOrcSink(model, properties);
                    break;
                case HDFSPARQUET:
                    sinkToInitAndStart = new HdfsParquetSink(model, properties);
                    break;
                case HBASE:
                    sinkToInitAndStart = new HbaseSink(model, properties);
                    break;
                case HIVE:
                    sinkToInitAndStart = new HiveSink(model, properties);
                    break;
                case OZONE:
                    sinkToInitAndStart = new OzoneSink(model, properties);
                    break;
                case SOLR:
                    sinkToInitAndStart = new SolRSink(model, properties);
                    break;
                case KAFKA:
                    sinkToInitAndStart = new KafkaSink(model, properties);
                    break;
                case KUDU:
                    sinkToInitAndStart = new KuduSink(model, properties);
                    break;
                case CSV:
                    sinkToInitAndStart = new CSVSink(model, properties);
                    break;
                case JSON:
                    sinkToInitAndStart = new JsonSink(model, properties);
                    break;
                case AVRO:
                    sinkToInitAndStart = new AvroSink(model, properties);
                    break;
                case PARQUET:
                    sinkToInitAndStart = new ParquetSink(model, properties);
                    break;
                case ORC:
                    sinkToInitAndStart = new ORCSink(model, properties);
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
