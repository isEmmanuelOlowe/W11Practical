import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* Reverse order to sort by largest
*/
public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

  /**
  * Counts the occurences of a key.
  *
  * @param key the key which is being reduced.
  * @param values the values of the key which have been found
  * @param output the output of the reduce
  * @throws IOException an input/output error has occurd
  * @throws InterruptedException the thread has been interrupted
  */
  public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

    // The value is the users name plus thier location.
    //to get correct number
    long num = key.get() * - 1;
    for (Text value : values) {
      //output the total oocurences of the key by largest
      output.write(value, new LongWritable(num));
    }
  }
}
