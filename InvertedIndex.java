import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
        public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); //存储单词和URI的组合
        private Text valueInfo = new Text(); //存储词频
        private FileSplit split; //存储Split对象
        private String pattern = "[^a-zA-Z0-9-]" ;        // 统计词频时，需要去掉标点符号等符号，此处定义表达式   
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //获得<key,value>对所属的FileSplit对象
			split = (FileSplit) context.getInputSplit();
        String line = value.toString();
        line = line.replaceAll(pattern, " ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) { 
				//key值由单词和URI组成，如"MapReduce，1.txt" 
                keyInfo.set(itr.nextToken() + ":" + split.getPath().getName());
                 // 词频初始为1
				valueInfo.set("1"); 
                context.write(keyInfo, valueInfo);
            }
        }
    }
        public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
			//统计词频
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf(":");
			//重新设置value值由URI和词频组成
            info.set(key.toString().substring(splitIndex + 1) + "," + sum);
			//重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }
 
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fileList = new String();
			//生成文档列表
            for (Text value : values) {
                fileList += "(" + value.toString() + ")";
            } 
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
    Configuration conf=new Configuration();
    String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
    if(otherArgs.length!=2){
    System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf,"InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

