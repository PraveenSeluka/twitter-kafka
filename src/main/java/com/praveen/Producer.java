package com.praveen;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import twitter4j.*;
import twitter4j.StatusListener;
import twitter4j.conf.*;
import twitter4j.util.*;
import twitter4j.auth.*;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import static net.sourceforge.argparse4j.impl.Arguments.store;

import org.apache.kafka.clients.producer.*;


public class Producer
{
    public static void main( String[] args )
    {
        ArgumentParser parser = argParser();
        try {
            Namespace res = parser.parseArgs(args);

            final String topic = res.getString("topic");
            String filters = res.getString("filters");
            String configFile = res.getString("conf");

            final Properties props = new Properties();
            try {

                String confFile = "conf/producer.conf";
                if(configFile!=null && configFile.length()>0)
                    confFile = configFile;
                FileInputStream in = new FileInputStream(confFile);
                props.load(in);
                in.close();
            }catch (IOException io) {
                io.printStackTrace();
            }

            List<String> producerProps = res.getList("producerConfig");

            if (producerProps != null)
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }

            if(filters!=null && filters.length()>0)
                props.setProperty("filters",filters);
            if(topic!=null && topic.length()>0)
                props.setProperty("topic",topic);

            props.list(System.out);

            final KafkaProducer<String,String> producer = new KafkaProducer(props);

            StatusListener listener = new StatusListener(){
                int messageCount = 0;
                public void onStallWarning(StallWarning warn) {
                    System.err.println("warning");
                }
                public void onScrubGeo(long a, long b) {
                    System.err.println("scrub");
                }
                public void onStatus(Status status) {
                    double lat=0.0;
                    double lon=0.0;
                    if(status.getGeoLocation() !=null) {
                        lat = status.getGeoLocation().getLatitude();
                        lon = status.getGeoLocation().getLongitude();
                    }
                    String userName = status.getUser().getName();
                    String tweet = status.getText();
                    int favCount = status.getFavoriteCount();
                    int retweetCount = status.getRetweetCount();
                    ProducerRecord<String,String> record = new ProducerRecord(props.getProperty("topic"),userName, tweet);
                    producer.send(record);
                    messageCount++;
                    if(messageCount%100 == 0) {
                        System.out.println("Total messages written : " + messageCount);
                        System.out.println("Sample tweet : " + status.getUser().getName() + " : " + status.getText()+ " "+status.getFavoriteCount()+" "+status.getRetweetCount()+" " + lat + " " + lon);
                    }
                }
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true);
            cb.setOAuthConsumerKey(props.getProperty("consumerKey"));
            cb.setOAuthConsumerSecret(props.getProperty("consumerSecret"));
            cb.setOAuthAccessToken(props.getProperty("accessToken"));
            cb.setOAuthAccessTokenSecret(props.getProperty("accessTokenSecret"));

            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            twitterStream.addListener(listener);

            if (props.getProperty("filters") == null || props.getProperty("filters").length()==0)
                twitterStream.sample();
            else
                twitterStream.filter(filters);

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("Twitter Kafka Connector")
                .defaultHelp(true)
                .description("This tool can consumer twitter data (based on filters) and publish to Kafka");

        parser.addArgument("--conf")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONF")
                .dest("conf")
                .help("Config file. Template available at conf/producer.conf");

        parser.addArgument("--topic")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

          parser.addArgument("--producer-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("producerConfig")
                .help("kafka producer related configuaration properties like bootstrap.servers,client.id etc..");

        parser.addArgument("--filters")
                .action(store())
                .required(false)
                .metavar("FILTERS")
                .type(String.class)
                .dest("filters")
                .help("Comma separated list of keywords to search");

        return parser;
    }
}
