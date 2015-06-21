
import java.io.*;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class mapjoin {

	//The Mapper classes and  reducer  code
	public static class Map extends Mapper<LongWritable, Text, Text,NullWritable >{
		String mymovieid;
		HashMap<String,String> mymap = new HashMap<String,String>();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);

			Configuration conf = context.getConfiguration();
			mymovieid = conf.get("movieid"); // to retrieve movieid set in main method
			Path[] localPaths = context.getLocalCacheFiles();
			for(Path myfile:localPaths)
			{
				String line = "";
				String nameofFile = myfile.getName();
				File file = new File(nameofFile+"");
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				while((line = br.readLine())!=null)
				{
					String[] arr = line.split("::");
				    String info = arr[1]+'\t'+arr[2];//gender and age
					mymap.put(arr[0],info);
				}
			}

		}


		//private Text rating;
		private Text userid = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			String id = mydata[0];
			String infor = null;
			if(mymap.containsKey(id))
			{
				if(mydata[1].equals(mymovieid)&&(Float.parseFloat(mydata[2])>=4.0))
				{
					infor = id+"\t"+mymap.get(id);
					userid.set(infor);
					context.write( userid,NullWritable.get());
				}

			}
		}


	}



	//The reducer class	
	public static class Reduce extends Reducer<Text,NullWritable,Text,NullWritable> {
//		private Text result = new Text();
//		private Text myKey = new Text();
		//note you can create a list here to store the values

		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {

				context.write(key,NullWritable.get());

		}		
	}


	//Driver code
	public static void main(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: JoinExample <in> <out> <anymovieid>");
			System.exit(2);
		}
		


		conf.set("movieid", otherArgs[2]); //setting global data variable for hadoop

		// create a job with name "joinexc" 
		Job job = new Job(conf, "mapjoin"); 
		job.setJarByClass(mapjoin.class);
		job.addCacheFile(new URI(NAME_NODE + "/user/hue/users/users.dat"));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);


		FileInputFormat.addInputPath(job, new Path(otherArgs[0]) );
		job.setOutputValueClass(NullWritable.class);
		// set output value type
		job.setOutputKeyClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);


	}
}



