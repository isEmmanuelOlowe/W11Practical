import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParsingException;
import java.io.InputStreamReader;
import java.io.StringReader;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

/**
* Takes a line of input as json object and gets user and location.
*/
public class TweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  /**
  * Maps the user + location to one occurence.
  *
  * @param key character offest within the file at the start of the line
  * @param value the line of text from the file
  * @param output the output to the reducer
  * @throws IOException an input/output error has occured
  * @throws InterruptedException the thread has been interrupted.
  * @throws JsonParsingException invalid json has been found.
  */
  public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException, JsonParsingException {

    //converts the line of file to string
    String line = value.toString();
    //converts line to json
    JsonObject tweet = Json.createReader(new StringReader(line)).readObject();
    //gets the user object from within the tweet object
    JsonObject user = (JsonObject) tweet.get("user");
    //ensures the user object actually exists or null pointer error will be thrown
    if (user != null) {
      //removes new lines and returns and gets the screen_name
      String username = user.getString("screen_name").replace("\n", " ").replace("\r", " ");
      //gets the location of location as json value
      JsonValue jsonLocation = user.get("location");
      //checks they dont have a null location
      if (jsonLocation.getValueType() != ValueType.NULL) {
        //removes new lines and returns and gets the location and makes output key
        String userLocation = username + " (" + user.getString("location").replace("\n", " ").replace("\r", " ") + ")";
        output.write(new Text(userLocation), new LongWritable(1));
      }
    }
  }
}
