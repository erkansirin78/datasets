package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit
{
	public static void main(String[] args) throws Exception 
	{
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> data = env.readTextFile("/home/jivesh/avg");
		
		                // month, product, category, profit, count
		DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12,1]
		                                                                                                     //        [June,Category4,Perfume,10,1]
		
		// groupBy 'month'                                                                                           
		DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(0).reduce(new Reduce1());   
             																// June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce			
		                                                        // reduced = { [Category4,Perfume,22,2] ..... }
		        // month, avg. profit  
DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>()
			{
				public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input)
					{
						return new Tuple2<String, Double>(input.f0, new Double((input.f3*1.0)/input.f4));
					}
				});
		
      profitPerMonth.print();

		// execute program
		env.execute("Avg Profit Per Month");
	}
                                                                                           
	 // *************************************************************************
    // USER FUNCTIONS                                                                                  // pre_result  = Category4,Perfume,22,2        
    // *************************************************************************
	
	public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>>
	{
		public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
																	   Tuple5<String, String, String, Integer, Integer> pre_result)
		{
	return new Tuple5<String, String, String, Integer, Integer>(current.f0,current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4); 
		}
	}

	public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> 
	{
		public Tuple5<String, String, String, Integer, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
		{
			String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			// ignore timestamp, we don't need it for any calculations
			return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1); 
		}                                                            //    June    Category5      Bat                      12 
	}
}
