import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* Counts the occurences of a users tweets in a dataset.
*/
public class TweetCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  /**
  * Counts the occurences of a key.
  *
  * @param key the key which is being reduced.
  * @param values the values of the key which have been found
  * @param output the output of the reduce
  * @throws IOException an input/output error has occurd
  * @throws InterruptedException the thread has been interrupted
  */
  public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

    // The key is the users name plus thier location.
    // The values shall just be one.
    int sum = 0;
    for (LongWritable value : values) {
      long l = value.get();
      sum += l;
    }
  //output the total oocurences of the key
  output.write(key, new LongWritable(sum));
  }
}
