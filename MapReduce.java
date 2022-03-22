import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;


public class MapReduce {
    public static void main(String[] args) {
        reuterDb_to_file();
        processedDB_to_file();
    }
    public static MongoDatabase mongoConnection() {
        //mongo connection with ReuterDb
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://root," +
                "" +
                "" +
                ""));
        MongoDatabase reuterDB = mongoClient.getDatabase("ProcessedDB");
        return reuterDB;
    }

    public static void reuterDb_to_file(){
        MongoDatabase conn = mongoConnection();
        MongoCollection artcleCollection = conn.getCollection("news_articles");

        try (MongoCursor<Document> cur = artcleCollection.find().iterator()) {
            PrintWriter reuter_body = new PrintWriter("reuterDB_body.txt", "UTF-8");
            while (cur.hasNext()) {
                JSONObject tweet_json = new JSONObject(cur.next().toJson());
                if (tweet_json.has("Body")) {
                    String output = tweet_json.getString("Text");
                    reuter_body.println(output);
                }
            }
        } catch (FileNotFoundException | JSONException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void processedDB_to_file(){
        MongoDatabase conn = mongoConnection();
        MongoCollection canadaCollection  = conn.getCollection("Canada");
        MongoCollection educationCollection  = conn.getCollection("Education");
        MongoCollection hockeyCollection  = conn.getCollection("Hockey");
        MongoCollection temperatureCollection  = conn.getCollection("Temperature");
        MongoCollection weatherCollection  = conn.getCollection("Weather");

        PrintWriter processed_text = null;
        try {
            processed_text = new PrintWriter("processedDB.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        MongoCollection[] collections = {canadaCollection, educationCollection, hockeyCollection, temperatureCollection,
                weatherCollection};
        for (MongoCollection collection: collections) {
            try {
                MongoCursor<Document> cur = collection.find().iterator();
                while (cur.hasNext()) {
                    JSONObject tweet_json = new JSONObject(cur.next().toJson());
                    processed_text.println(tweet_json.get("Text"));
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
}
