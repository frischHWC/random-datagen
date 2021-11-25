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
                    sinkToInitAndStart = new HdfsCsvSink();
                    break;
                case HDFSJSON:
                    sinkToInitAndStart = new HdfsJsonSink();
                    break;
                case HDFSAVRO:
                    sinkToInitAndStart = new HdfsAvroSink();
                    break;
                case HDFSORC:
                    sinkToInitAndStart = new HdfsOrcSink();
                    break;
                case HDFSPARQUET:
                    sinkToInitAndStart = new HdfsParquetSink();
                    break;
                case HBASE:
                    sinkToInitAndStart = new HbaseSink();
                    break;
                case HIVE:
                    sinkToInitAndStart = new HiveSink();
                    break;
                case OZONE:
                    sinkToInitAndStart = new OzoneSink();
                    break;
                case SOLR:
                    sinkToInitAndStart = new SolRSink();
                    break;
                case KAFKA:
                    sinkToInitAndStart = new KafkaSink();
                    break;
                case KUDU:
                    sinkToInitAndStart = new KuduSink();
                    break;
                case CSV:
                    sinkToInitAndStart = new CSVSink();
                    break;
                case JSON:
                    sinkToInitAndStart = new JsonSink();
                    break;
                case AVRO:
                    sinkToInitAndStart = new AvroSink();
                    break;
                case PARQUET:
                    sinkToInitAndStart = new ParquetSink();
                    break;
                case ORC:
                    sinkToInitAndStart = new ORCSink();
                    break;
                 default:
                     logger.info("The sink " + sink + " provided has not been recognized as an expected sink");
                     break;
            }

            if(sinkToInitAndStart != null) {
                logger.info(sinkToInitAndStart.getClass().getSimpleName() + " is added to the list of sink");
                sinkToInitAndStart.init(model);
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
