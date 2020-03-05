package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.log4j.Logger;

import java.util.List;


public interface SinkInterface {

    // TODO: Remake sinks to use Row instead of MainModel

    Logger logger = Logger.getLogger(SinkInterface.class);

    void sendOneBatchOfRows(List<Row> rows);

    void init(Model model);

    void terminate();
}
