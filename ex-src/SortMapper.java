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
* Takes a line of input extracts the users number of tweets and their user + location string
*/
public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  /**
  * Extracts the number of tweet.
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
    String[] data = line.split("\\s");
    String number = data[data.length - 1];
    String userLocation = line.substring(0, line.length() - number.length());
    //multiplied by negative 1 to reverse the order
    Long num = Long.parseLong(number) *  - 1;
    output.write(new LongWritable(num), new Text(userLocation));
  }
}
