package org.example.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.batch.BatchWordCount;

public class FlinkWordCountFromText {
    public static void main(String[] args) throws Exception{
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setParallelism(8);
//        //定义文件路径
//        String inputPath = "/Users/apple/Documents/FlinkProject/src/main/resources/hello.txt";
//        DataStream<String> inputStream = env.readTextFile(inputPath);
        //定义socket流
        //一般来讲,程序里我们是不可能把参数写死的,而是通过启动时参数
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9000);



        DataStream<Tuple2<String, Integer>> resultStream = inputStream.flatMap(new BatchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        env.execute();
    }
}
