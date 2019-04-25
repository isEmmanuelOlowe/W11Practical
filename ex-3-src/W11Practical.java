import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import javax.json.stream.JsonParsingException;
import java.io.PrintStream;
import org.apache.commons.io.output.NullOutputStream;

/**
* Uses Map-Reduce to total the Tweets made by user at location with a specific time period.
*/
public class W11Practical {

  /**
  * Takes in command line arguments and executes program.
  *
  * @param args the command line arguments provided.
  * @throws IOException input/output error has occured.
  */
  public static void main(String[] args) throws IOException {
    long startTime = System.nanoTime();
    PrintStream out = System.out;
    System.setOut(new PrintStream(new NullOutputStream()));
    //checks that the correct command line arguments have been passed
    if (args.length < 3) {
      System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_directory> <map_tasks>");
      System.exit(1);
    }

    //gets the input and output paths
    String input_path = args[0];
    String output_path = args[1];
    int tasks = Integer.valueOf(args[2]);
    // Setup new Job and Configuration
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "W11Practical");

    // Specify input and output paths
    FileInputFormat.setInputPaths(job, new Path(input_path));
    FileOutputFormat.setOutputPath(job, new Path(output_path));

    //Sets Mapper to tweet mapper
    job.setMapperClass(TweetMapper.class);

    //Specify output types produced by mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    //Sets the reducer class
    job.setReducerClass(TweetCountReducer.class);

    //Specify the output types produced by reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    LocalJobRunner.setLocalMaxRunningMaps(job, tasks);
    try {
      job.waitForCompletion(true);
    }
    catch (ClassNotFoundException e) {
      System.out.println(e.getMessage());
    }
    catch (IOException e) {
      System.out.println(e.getMessage());
   }
   catch (InterruptedException e) {
     System.out.println(e.getMessage());
   }
   catch (JsonParsingException e) {
    System.out.println(e.getMessage());
   }
   long endTime = System.nanoTime();

   System.setOut(out);
   System.out.println("Map Tasks Running Concurrently: " + tasks + ". Completed in: " + (endTime - startTime) + " ns.");
  }
}
