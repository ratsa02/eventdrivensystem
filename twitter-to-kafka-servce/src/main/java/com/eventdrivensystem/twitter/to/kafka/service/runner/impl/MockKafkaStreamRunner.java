package com.eventdrivensystem.twitter.to.kafka.service.runner.impl;

import com.eventdrivensystem.twitter.to.kafka.service.Exception.TwitterToKafkaServiceException;
import com.eventdrivensystem.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.eventdrivensystem.twitter.to.kafka.service.listner.TwitterToKafkaStatusListner;
import com.eventdrivensystem.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterToKafkaStatusListner twitterToKafkaStatusListner;

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData configData, TwitterToKafkaStatusListner statusListner) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterToKafkaStatusListner = statusListner;

    }

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };


    private static final String tweetAsRawJson =  "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";


    private static final String TWITTER_STATUS_DATE_FORMAT= "EEE MMM dd HH:mm:ss zz yyyy";




    @Override
    public void start() throws TwitterException, IOException, URISyntaxException {

        final String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        final int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        final int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMS = twitterToKafkaServiceConfigData.getMockSleeps();

        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords,minTweetLength,maxTweetLength,long sleepTimeMS);



    }

    private void simulateTwitterStream(String[] keywords,int minTweet,int maxTweet,long sleepInMS){

        Executors.newSingleThreadExecutor().submit(()->{
            try {
                while(true){
                    String formatterTweetAsRawJson = getFomattedTweet(keywords,minTweet,maxTweet);
                    Status status = TwitterObjectFactory.createStatus(formatterTweetAsRawJson);
                    twitterToKafkaStatusListner.onStatus(status);
                    sleep(sleepInMS);
                }
            }catch(TwitterException | InterruptedException e){
                LOG.error("Error Creating twitter status!",e);
            }

        });

    }

    private void sleep(long sleepInMS) throws InterruptedException {
        try {
            Thread.sleep(sleepInMS);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!!");
        }
    }

    private String getFormatterTweet(String[] keywords,int minTweet,int maxTweet){
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)
                ),
        }
    }
}
