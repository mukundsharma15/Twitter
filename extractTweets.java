import com.mongodb.client.MongoCursor;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class extractTweets {

    public static void main(String[] args) {

        // mongo connection
        MongoClient client = mongoConnection();
        MongoDatabase rawDBConnection = client.getDatabase("RawDB");

        //get data from required collection
        String query_tag = "Cricket";
        MongoCollection rawCollection = rawDBConnection.getCollection(query_tag);

        // twitter connection
        Twitter twitter_obj = TwitterConnection();

        // get tweets and inset into rawDB
        try {
            Query query = new Query(query_tag);
            query.setCount(100);
            QueryResult result;
            result = twitter_obj.search(query);
            List<Status> tweets = result.getTweets();
            for (Status tweet : tweets) {
                String statusJson = DataObjectFactory.getRawJSON(tweet);
                JSONObject JSON_complete = new JSONObject(statusJson);
                Document doc = Document.parse(JSON_complete.toString());
                // insert tweets in rawDB
                rawCollection.insertOne(doc);
            }
            processedData(client, rawCollection);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }

    public static MongoClient mongoConnection() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI(""));
        return mongoClient;
    }

    public static Twitter TwitterConnection() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("jeik7aMqi4bzjTeIIolQtIRRr")
                .setOAuthConsumerSecret("Kd0K7YdcwVLJtg7ZQcmfAcnf9ju92WQmgIsfHvMXsiPnZzmcZE")
                .setOAuthAccessToken("1455693448670031875-rlCLxvhatgl4yPsT4TUmKhHZ4R1DVr")
                .setOAuthAccessTokenSecret("zKxzvr05WizZWpbMTwyamXE3g88AwXuCHkwszX1gmIFzd");
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        return twitter;
    }

    public static void processedData(MongoClient client, MongoCollection rawCollection) {

        // now query rawDB to extract tweets and apply regex before inserting in processedDb
        MongoDatabase processedDB = client.getDatabase("ProcessedDB");
        MongoCollection processedCollection = processedDB.getCollection("Cricket");

        try (MongoCursor<Document> cur = rawCollection.find().iterator()) {
            while (cur.hasNext()) {
                String tweet_str = cur.next().toJson();
                JSONObject tweet_json = new JSONObject(tweet_str);
                JSONObject new_json = new JSONObject();
                String emoji_regex = "[\\ud83c\\udc00-\\ud83c\\udfff]|[\\ud83d\\udc00-\\ud83d\\udfff]|[\\u2600-\\u27ff]";
                new_json.append("Tweet-Text", tweet_json.get("full_text").toString().
                        replaceAll("[^a-zA-Z0-9]", " ").replaceAll(emoji_regex, " "));
                new_json.append("User", tweet_json.get("user"));
                Document final_doc = Document.parse(new_json.toString());
                processedCollection.insertOne(final_doc);

            }
        }
    }
}
