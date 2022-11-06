package com.eventdrivensystem.twitter.to.kafka.service.runner.impl;

import com.eventdrivensystem.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.eventdrivensystem.twitter.to.kafka.service.listner.TwitterToKafkaStatusListner;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;


@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2StreamHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterV2StreamHelper.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterToKafkaStatusListner twitterToKafkaStatusListner;

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
    "}";

    private static final String TWITTER_SATATUS_DATE_FORMAT="EEE MMM dd HH:mm:ss zzz yyyy";

    public TwitterV2StreamHelper(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterToKafkaStatusListner twitterToKafkaStatusListner) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterToKafkaStatusListner = twitterToKafkaStatusListner;
    }


    void connectStream(String bearerToken) throws IOException, URISyntaxException, JSONException{

        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

        URIBuilder uriBuilder = new URIBuilder(twitterToKafkaServiceConfigData.getTwitterV2BaseUrl());
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization",String.format("Bearer %s",bearerToken));

        HttpResponse  response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();

        if(null != entity){
            BufferedReader reader= new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            while(null != line){
                line = reader.readLine();
                if(!line.isEmpty()){
                    String tweet = getFormatedTweet(line);
                    Status status = null;
                    try{
                        status = String.valueOf(TwitterObjectFactory.createStatus(tweet));
                    } catch (TwitterException e) {
                        LOG.info("Couldn't create status for text: ",tweet,e);
                    }
                    if(status != null){
                        twitterToKafkaStatusListner.onStatus(status);
                    }
                }
            }

        }
    }

    void setupRules(String bearerToken,Map<String,String> rules) throws IOException,URISyntaxException{
        List<String> existingRules = getRules(bearerToken);
        if(existingRules.size() > 0){
            deleteRule(bearerToken,existingRules);
        }
        createRules(bearerToken,rules);
        LOG.info("Created rules for tweeterStream",rules.keySet().toArray());

    }
    void createRules(String bearerToke,Map<String,String> rules) throws IOException,URISyntaxException{



    }
    private String getFormatedTweet(String data){
        JSONObject jsonData = (JSONObject) new JSONObject(data).get("data");
        String[] params = new String[]{
                ZonedDateTime.parse(jsonData.get("created_at").toString()).withZoneSameInstant(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(TWITTER_SATATUS_DATE_FORMAT, Locale.ENGLISH)),
                jsonData.get("id").toString(),
                jsonData.get("text").toString().replaceAll("\"","\\\\\""),
                jsonData.get("author_id").toString(),};

        return formatTweetAsjsonWithParams(params);

    }
    private String formatTweetAsjsonWithParams(String[] params){
        String tweet = tweetAsRawJson;
        for(int i =0;i< params.length; i++){
            tweet = tweet.replace("{"+i+"}",params[i]);
        }
        return tweet;
    }

    private String getFormattedString(String string,Map<String,String> rules){
        StringBuilder sb = new StringBuilder();

        if(rules.size() !== 1){
            String key = rules.keySet().iterator().next();
            return String.format(string,"{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        }else{
            for(Map.Entry<String,String> entry : rules.entrySet()){
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string,result.substring(0,result.length() - 1));
        }
    }






}
