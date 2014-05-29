package cn.com.gxdgroup.dataplatform.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by wq on 5/23/14.
 */
public class JavaWorldCount {

    public static JavaRDD<String> splitrdd(JavaPairRDD<LongWritable, Text> lines1) {
        JavaRDD<String> split = lines1.flatMap(new FlatMapFunction<Tuple2<LongWritable,Text>, String>(){
            public Iterable<String> call(Tuple2<LongWritable, Text> pair){
                return Arrays.asList(pair._2().toString().split(" "));
            }

        });
        return split;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <master> <file>");
            System.exit(1);
        }
        JavaSparkContext ctx = new JavaSparkContext(args[0], "WqWordCount",
                System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));
        JavaRDD<String> file = ctx.textFile(args[1], 1);

        Configuration conf = new Configuration();
        //JavaPairRDD<LongWritable, Text> lines1 = ctx.newAPIHadoopFile(args[1],ETLLineInputFormat.class, LongWritable.class, Text.class, new Job().getConfiguration());

        System.out.println(file.count());

//        JavaRDD<String> split = lines.flatMap(new FlatMapFunction<Tuple2<LongWritable,Text>, String>(){
//            public Iterable<String> call(Tuple2<LongWritable, Text> pair){
//                return Arrays.asList(pair._2().toString().split(" "));
//            }
//
//        });

        JavaRDD<String> words = getStringJavaRDD(file);


        JavaPairRDD<String, Integer> maper = words.map(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

		    /*JavaPairRDD<String,Integer> Maper = lines1.map(new PairFunction<Tuple2<LongWritable,Text>,String,Integer>(){
		    	Text keys = new Text();
		    	IntWritable values = new IntWritable();
		    	Pattern pattern  = Pattern.compile("\\W+");
				public Tuple2<String,Integer> call(Tuple2<LongWritable ,Text> pair){
					//Arrays.asList(pair._2().toString().split(""));
					for (String word : pair._2().toString().split("\\W+")) {
					      if (word.length() > 0) {
					    	  keys.set(word);
					    	  values.set(1);
					    	  return new Tuple2<String, Integer>(word,1);
					      }
					}
					return null;
				}
		    });*/


        JavaPairRDD<String, Integer> counts = maper.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //counts.saveAsHadoopFile("hdfs://cloud216:8020/user/boco/wq.txt", Text.class, Text.class, FileOutputFormat.class);
        //counts.saveAsNewAPIHadoopFile("hdfs://cloud216:8020/user/boco/wq.txt", Text.class, IntWritable.class, OutputTest.class);
        counts.saveAsTextFile("hdfs://cloud216:8020/user/boco/wq.txt");
		    /*List<Tuple2<String, Integer>> output = Molecules.collect();
		    for (Tuple2 tuple : output) {
		       System.out.println("--------------------");
		      System.out.println(tuple._1 + ": " + tuple._2);

		    }*/

        System.exit(0);


    }

    public static JavaRDD<String> getStringJavaRDD(JavaRDD<String> file) {
        return file.flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
            });
    }
}
