import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReuterTweets {

    // extract title and body from both the given files
    public static void main(String[] args) {
        //String [] files = new String[];
        String[] files = {"reut2-014.sgm", "reut2-009.sgm"};
        for (String file: files) {
            extractTitleFromFile(file);
        }
    }

    public static MongoDatabase mongoConnection() {
        //mongo connection with ReuterDb
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://root:password12345678@cluster0-" +
                "shard-00-00.7asei.mongodb.net:27017," +
                "cluster0-shard-00-01.7asei.mongodb.net:27017," +
                "cluster0-shard-00-02.7asei.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-oeu3y8-" +
                "shard-0&authSource=admin&retryWrites=true&w=majority"));
        MongoDatabase reuterDB = mongoClient.getDatabase("ReuterDb");
        return reuterDB;
    }

    public static void extractTitleFromFile(String file_name){
        try {
            Scanner input = new Scanner(new FileReader(file_name));
            List<Document> title = new ArrayList<>();
            while (input.hasNextLine()) {
                String line = input.nextLine();
                try{
                    //extracting title from txt file
                    Pattern tittle_pattern = Pattern.compile("<TITLE>(.+?)</TITLE>", Pattern.DOTALL);
                    Matcher matcher1 = tittle_pattern.matcher(line);
                    matcher1.find();
                    JSONObject json_doc = new JSONObject();
                    json_doc.put("Title", matcher1.group(1));

                    // extracting body of sgm file
                    List<String> body_data = new ArrayList<String>();
                    String body_line = input.nextLine();
                    // regex logic for extracting body
                    Boolean has = body_line.toLowerCase().contains("<BODY>".toLowerCase());
                    if (has){
                        body_data.add(body_line.split("<BODY>")[1]);
                        while(input.hasNextLine()){
                            String line1 = input.nextLine();
                            if (line1.toLowerCase().contains("</BODY>".toLowerCase())){
                                break;
                            }else{
                                body_data.add(line1);
                            }
                        }
                    }
                    if (body_data.size() != 0){
                        // merge list to string
                        String result = String.join("\n", body_data);
                        json_doc.put("Body", result);
                    }

                    Document doc = Document.parse(json_doc.toString());
                    // adding document in Document list
                    title.add(doc);
                }catch (IllegalStateException e){}
            }
            MongoDatabase reuterDB = mongoConnection();
            // finally inserting in DB
            MongoCollection<Document> reuterCollection = reuterDB.getCollection("news_articles");
            reuterCollection.insertMany(title);
        } catch (FileNotFoundException e) {}
    }
}
