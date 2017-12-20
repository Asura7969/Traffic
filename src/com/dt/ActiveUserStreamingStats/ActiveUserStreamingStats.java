package com.dt.ActiveUserStreamingStats;

import com.dt.ExecuteCallBack;
import com.dt.JDBCWrapper;
import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.ResultSet;
import java.util.*;

/**
 * Created by gongwenzhou on 2017/12/20.
 * 实时统计活跃用户数
 */
public class ActiveUserStreamingStats {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("ActiveUserStreamingStats").setMaster("local[5]");
        final String checkpoint = "hdfs://mycluster:8020/checkpoint/activeUser";

        JavaStreamingContextFactory jscFactory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
                return createFunc(checkpoint,conf);
            }
        };
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpoint, jscFactory);

        //创建kafka元数据
        Map<String,String> kafkaParameters = new HashMap<String,String>();
        kafkaParameters.put("metadata.broker.list", "192.168.126.111:9092,192.168.126.112:9092,192.168.126.113:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("activeUser");


        JavaPairInputDStream<String, String> userStream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParameters,
                topics);


        JavaPairDStream<String, String> filteredadStreaming = userStream.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> userRDD) throws Exception {

                final List<String> blackNameList = new ArrayList<String>();
                JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                jdbcWrapper.doQuery("select * from tb_fp_blackName", null, new ExecuteCallBack() {
                    @Override
                    public void resultCallBack(ResultSet result) throws Exception {
                        while (result.next()) {
                            blackNameList.add(result.getString(2));
                        }
                    }
                });

                List<Tuple2<String, Boolean>> blackNamesTup = new ArrayList<Tuple2<String, Boolean>>();
                for (String userid : blackNameList) {
                    blackNamesTup.add(new Tuple2<String, Boolean>(userid, true));
                }

                JavaSparkContext jsc = new JavaSparkContext(userRDD.context());

                JavaPairRDD<String, Boolean> blackRDD = jsc.parallelizePairs(blackNamesTup);

                JavaPairRDD<String, Tuple2<String, String>> rdd2Pair = userRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> rdd) throws Exception {
                        String userid = rdd._2().split(" ")[2];
                        return new Tuple2<>(userid, rdd);
                    }
                });

                JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = rdd2Pair.leftOuterJoin(blackRDD);
                JavaPairRDD<String, String> resultRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> rdd) throws Exception {

                        Optional<Boolean> flag = rdd._2._2;


                        return flag.isPresent() ? flag.get() : false;
                    }
                }).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD) throws Exception {
                        return filteredRDD._2._1;
                    }
                });

                return resultRDD;

            }
        });

    }

    private static JavaStreamingContext createFunc(String checkpoint, SparkConf conf) {
        SparkConf sparkConf = conf;

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        jsc.checkpoint(checkpoint);
        return jsc;
    }


}
