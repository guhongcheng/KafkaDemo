package org.example.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //创建运行时环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //读取数据源
        String inputSource = "/Users/apple/Documents/FlinkProject/src/main/resources/hello.txt";
        DataSource<String> stringDataSource = env.readTextFile(inputSource);

        //切分数据
        DataSet<Tuple2<String,Integer>> resultSet = stringDataSource.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        resultSet.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word: words){
                out.collect(new Tuple2<>(word,1));
            }

        }
    }
}
