# twitter-kafka

**Build** : `mvn package`

Edit the conf/producer.conf file and set your twitter credentials

**Run** : `java -cp target/twitter-kafka-1.0-SNAPSHOT.jar:target/* com.praveen.Producer`

**usage** : Twitter Kafka Connector [-h] [--conf CONF] --topic TOPIC [--producer-props PROP-NAME=PROP-VALUE [PROP-NAME=PROP-VALUE ...]] [--filters FILTERS]

This tool can consumer twitter data (based on filters) and publish to Kafka

optional arguments:

  -h, --help             show this help message and exit
  
  --conf CONF            Config file. Template available at conf/producer.conf
  
  --topic TOPIC          produce messages to this topic
  
  --producer-props PROP-NAME=PROP-VALUE [PROP-NAME=PROP-VALUE ...]
                         kafka producer related configuaration properties like bootstrap.servers,client.id etc..
                         
  --filters FILTERS      Comma separated list of keywords to search
