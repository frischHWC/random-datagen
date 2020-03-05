package com.cloudera.frisch.randomdatagen.config;

import org.junit.Test;

public class PropertiesLoaderTest {

    @Test
    public void checkConfigPropertiesWellLoaded() {
        PropertiesLoader.getProperty("test");
    }
}
