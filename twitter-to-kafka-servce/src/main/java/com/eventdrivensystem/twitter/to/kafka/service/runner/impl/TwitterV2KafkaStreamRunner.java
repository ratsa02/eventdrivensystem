package com.eventdrivensystem.twitter.to.kafka.service.runner.impl;

import com.eventdrivensystem.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.eventdrivensystem.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {



    private static final Logger LOG = LoggerFactory.getLogger(TwitterV2KafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData twittterToKafkaServiceConfigData;
    private final TwitterV2StreamHelper twitterV2StreamHelper;

    public TwitterV2KafkaStreamRunner(TwitterToKafkaServiceConfigData configData, TwitterV2StreamHelper streamHelper) {
        this.twittterToKafkaServiceConfigData = configData;
        this.twitterV2StreamHelper = streamHelper;
    }

    @Override
    public void start() {


        String bearerToken = twittterToKafkaServiceConfigData.getTwitterV2BearerToken();
        if(null != bearerToken){
            try{
                twitterV2StreamHelper.setupRules(bearerToken,getRules());
                twitterV2StreamHelper.connectStream(bearerToken);

            } catch (IOException |URISyntaxException e) {
                LOG.error("Error streaming tweets!",e);
                throw new RuntimeException("Error Streaming tweets", e);

            }

        }else{
            LOG.error("There was a problem getting the bearer token: Please make sure you set the TWITTER_BEARER_TOKEN env vairable");
            throw  new RuntimeException("here was a problem getting the bearer token: Please make sure you set the TWITTER_BEARER_TOKEN env vairable");
        }

    }
    private Map<String, String> getRules(){
        List<String> keywords = twittterToKafkaServiceConfigData.getTwitterKeywords();

        Map<String,String> rules = new HashMap<>();
        for(String keyword : keywords){
            rules.put(keyword, "keyword: "+keyword);
        }
        LOG.info("Created filter for twitter stream for keywords: {}",keywords);

        return rules;
    }
}
