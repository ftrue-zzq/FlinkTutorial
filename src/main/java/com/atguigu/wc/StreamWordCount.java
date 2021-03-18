package com.atguigu.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//核心概念：DataStream的理解
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(8);  //不设置的话  开发环境中（IDEA）默认是CPU的核数  集群环境中是配置文件flink-conf.yaml中parallelism.default参数中设置的值
        //env.disableOperatorChaining();


        //从文件中读取数据
        //String inputPath = "E:\\IdeaProjects\\Future\\FlinkJava\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        //DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        //基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("green")
                .keyBy(0)  //没有groupBy 与DataSet对比
                .sum(1).setParallelism(2).slotSharingGroup("red");
                //.disableChaining();


        resultStream.print().setParallelism(1);

        //执行任务
        env.execute();
    }
}
