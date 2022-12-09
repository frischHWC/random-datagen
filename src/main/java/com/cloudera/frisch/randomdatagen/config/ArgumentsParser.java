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
package com.cloudera.frisch.randomdatagen.config;

import lombok.Getter;

import java.util.*;


public class ArgumentsParser {

    private ArgumentsParser() { throw new IllegalStateException("Could not initialize this class"); }

    @Getter
    protected static final Map<args, Object> argsMap = new EnumMap<>(args.class);

    public static void parseArgs(String[] arguments) {
        argsMap.put(args.MODEL_FILE_PATH, String.valueOf(arguments[0]));
        argsMap.put(args.ROWS_PER_BATCH, Long.valueOf(arguments[1]));
        argsMap.put(args.NUMBER_OF_BATCHES, Long.valueOf(arguments[2]));
        List<sinks> sinksList = new ArrayList<>();
        for(int i=3; i<arguments.length; i++) {
            sinksList.add(stringToSink(arguments[i]));
        }
        argsMap.put(args.SINK_TO_FILL, sinksList);
    }

    private static sinks stringToSink(String sink) {
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

    public enum args {
        MODEL_FILE_PATH,
        ROWS_PER_BATCH,
        NUMBER_OF_BATCHES,
        SINK_TO_FILL
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
