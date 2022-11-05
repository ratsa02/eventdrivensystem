package com.eventdrivensystem.twitter.to.kafka.service;

import com.eventdrivensystem.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.eventdrivensystem.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import java.util.Arrays;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {


    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);


    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    public final StreamRunner streamRunner;
    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData, StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = configData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("App Started................!" );
        LOG.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0])));
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
    }
}
