import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import javax.json.stream.JsonParsingException;

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

    //checks that the correct command line arguments have been passed
    if (args.length < 2) {
      System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_directory>");
      System.exit(1);
    }

    //gets the input and output paths
    String input_path = args[0];
    String output_path = args[1];

    // Setup new Job and Configuration
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "W11Practical");

    // Specify input and output paths
    FileInputFormat.setInputPaths(job, new Path(input_path));
    if (args.length == 3 && args[2].equals("--sort")) {
      FileOutputFormat.setOutputPath(job, new Path("temp"));
    }
    else {
      FileOutputFormat.setOutputPath(job, new Path(output_path));
    }

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

   if (args.length == 3 && args[2].equals("--sort")) {
     // Setup new job2 and Configuration
     Configuration conf2 = new Configuration();
     Job job2 = Job.getInstance(conf2, "W11Practical");

     // Specify input and output paths
     FileInputFormat.setInputPaths(job2, new Path("temp"));
     FileOutputFormat.setOutputPath(job2, new Path(output_path));

     //Sets Mapper to tweet mapper
     job2.setMapperClass(SortMapper.class);

     //Specify output types produced by mapper
     job2.setMapOutputKeyClass(LongWritable.class);
     job2.setMapOutputValueClass(Text.class);

     //Sets the reducer class
     job2.setReducerClass(SortReducer.class);

     //Specify the output types produced by reducer
     job2.setOutputKeyClass(Text.class);
     job2.setOutputValueClass(LongWritable.class);

     try {
       job2.waitForCompletion(true);
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
   }
  }
}
