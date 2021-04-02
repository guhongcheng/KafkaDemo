/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.entity.WordCount;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> text = env.socketTextStream("localhost", 9000, "\n");
		DataStream<WordCount> windowcounts = text.flatMap(new FlatMapFunction<String, WordCount>() {
			@Override
			public void flatMap(String value, Collector<WordCount> out) throws Exception {
				for(String word:value.split(" ")){
					out.collect(new WordCount(word,1l));
				}
			}
		}).keyBy(0).sum(1);
		windowcounts.print().setParallelism(1);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}


//	public static class linespliter implements FlatMapFunction<String,Tuple2<String,Integer>>{
//
//		@Override
//		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//			for (String s : value.split(" ")){
//				out.collect(new Tuple2<>(s,1));
//			}
//		}
//	}
}
