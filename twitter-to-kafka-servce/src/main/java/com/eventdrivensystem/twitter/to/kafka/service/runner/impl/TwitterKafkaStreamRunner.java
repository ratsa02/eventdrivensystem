package com.eventdrivensystem.twitter.to.kafka.service.runner.impl;

import com.eventdrivensystem.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.eventdrivensystem.twitter.to.kafka.service.listner.TwitterToKafkaStatusListner;
import com.eventdrivensystem.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-v2-tweets",havingValue = "true", matchIfMissing = false )
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterToKafkaStatusListner twitterToKafkaStatusListner;
    TwitterStream twitterStream;


    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData, TwitterToKafkaStatusListner statusListner) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterToKafkaStatusListner = statusListner;
    }

    @Override
    public void start() throws TwitterException{
        twitterStream = new TwitterStreamFactory().getInstance() ;
        twitterStream.addListener(twitterToKafkaStatusListner);
        addFilter();
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Start filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream != null){
            LOG.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }
}
