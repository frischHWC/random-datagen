package com.cloudera.frisch.randomdatagen.config;


public class SinkParser {

    private SinkParser() { throw new IllegalStateException("Could not initialize this class"); }

    public static sinks stringToSink(String sink) {
        switch (sink.toUpperCase()) {
            case "HDFS-CSV": return sinks.HDFS_CSV;
            case "HDFS-JSON": return sinks.HDFS_JSON;
            case "HDFS-PARQUET": return sinks.HDFS_PARQUET;
            case "HDFS-ORC": return sinks.HDFS_ORC;
            case "HDFS-AVRO": return sinks.HDFS_AVRO;
            case "HBASE": return sinks.HBASE;
            case "HIVE": return sinks.HIVE;
            case "KAFKA": return sinks.KAFKA;
            case "OZONE": return sinks.OZONE;
            case "OZONE-PARQUET": return sinks.OZONE_PARQUET;
            case "OZONE-CSV": return sinks.OZONE_CSV;
            case "OZONE-JSON": return sinks.OZONE_JSON;
            case "OZONE-ORC": return sinks.OZONE_ORC;
            case "OZONE-AVRO": return sinks.OZONE_AVRO;
            case "SOLR": return sinks.SOLR;
            case "KUDU": return sinks.KUDU;
            case "CSV": return sinks.CSV;
            case "JSON": return sinks.JSON;
            case "AVRO": return sinks.AVRO;
            case "PARQUET": return sinks.PARQUET;
            case "ORC": return sinks.ORC;
            default: return null;
        }
    }

    public enum sinks {
        HDFS_CSV,
        HDFS_JSON,
        HDFS_PARQUET,
        HDFS_ORC,
        HDFS_AVRO,
        HBASE,
        HIVE,
        KAFKA,
        OZONE,
        OZONE_PARQUET,
        OZONE_CSV,
        OZONE_AVRO,
        OZONE_JSON,
        OZONE_ORC,
        SOLR,
        KUDU,
        CSV,
        JSON,
        AVRO,
        PARQUET,
        ORC
    }
}
