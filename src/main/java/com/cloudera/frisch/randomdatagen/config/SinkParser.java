package com.cloudera.frisch.randomdatagen.config;

import lombok.Getter;

import java.util.*;


public class SinkParser {

    private SinkParser() { throw new IllegalStateException("Could not initialize this class"); }

    public static sinks stringToSink(String sink) {
        switch (sink.toUpperCase()) {
            case "HDFS-CSV": return sinks.HDFSCSV;
            case "HDFS-JSON": return sinks.HDFSJSON;
            case "HDFS-PARQUET": return sinks.HDFSPARQUET;
            case "HDFS-ORC": return sinks.HDFSORC;
            case "HDFS-AVRO": return sinks.HDFSAVRO;
            case "HBASE": return sinks.HBASE;
            case "HIVE": return sinks.HIVE;
            case "KAFKA": return sinks.KAFKA;
            case "OZONE": return sinks.OZONE;
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
        HDFSCSV,
        HDFSJSON,
        HDFSPARQUET,
        HDFSORC,
        HDFSAVRO,
        HBASE,
        HIVE,
        KAFKA,
        OZONE,
        SOLR,
        KUDU,
        CSV,
        JSON,
        AVRO,
        PARQUET,
        ORC
    }
}
