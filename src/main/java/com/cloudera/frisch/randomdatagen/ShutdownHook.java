package com.cloudera.frisch.randomdatagen;

import com.cloudera.frisch.randomdatagen.sink.SinkInterface;

import java.util.List;

public class ShutdownHook extends Thread {
    private List<SinkInterface> sinks;

    public ShutdownHook(List<SinkInterface> sinks) {
        this.sinks=sinks;
    }

    @Override
    public void run()
    {
        sinks.forEach(SinkInterface::terminate);
    }
}
