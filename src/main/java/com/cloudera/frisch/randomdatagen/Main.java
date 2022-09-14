package com.cloudera.frisch.randomdatagen;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@SuppressWarnings("unchecked")
@SpringBootApplication
public class Main {

    @Autowired
    private ApplicationContext appContext;

    public static void main(String [] args) {
        SpringApplication.run(Main.class, args);
    }

    @Slf4j
    @Component
    public static class Shutdowner {

        @Autowired
        private ApplicationContext appContext;

        public void initiateShutdown(int returnCode){
            Shutdowner.log.error("Going to shutdown due to previous error");
            SpringApplication.exit(appContext, () -> returnCode);
        }

    }

}
