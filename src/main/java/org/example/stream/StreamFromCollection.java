package org.example.stream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.entity.SensorReading;

import java.util.Arrays;

public class StreamFromCollection {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1547768791l, 30.5)
                , new SensorReading("sensor2", 1547768721l, 30.1)
                , new SensorReading("sensor3", 1547768731l, 30.2)
        ));

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 5, 6, 8, 10);
        intStream.print();
        sensorStream.print();
        env.execute();
    }
}
