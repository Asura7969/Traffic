package com.dt;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
/**
 * 
 * 在线处理广告点击流
 * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
 * 
 * @author hp
 *
 */
public class AdClickedStreamingStats {

   public static void main(String[] args) {
      
      /*
       * 第一步：配置SparkConf：
       * 1，至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
       * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
       * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
       * 2，对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
       * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
       * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
       */
      final SparkConf conf = new SparkConf().setMaster("local[5]").
            setAppName("AdClickedStreamingStats");
      
      /*SparkConf conf = new SparkConf().setMaster("spark://Master:7077").
            setAppName("SparkStreamingOnKafkaReceiver");*/
      
      /*
       * 第二步：创建SparkStreamingContext：
       * 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
       * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容
       * 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
       * 所有需要在Driver重新启动后继续上衣系的状态，此时的状态恢复需要基于曾经的Checkpoint）；
       * 2，在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用下一个SparkStreamingContext
       * 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们获得一个重大的启发SparkStreaming框架也只是
       * Spark Core上的一个应用程序而已，只不过Spark Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
       */
      JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

      /*final String checkpointDirectory = "hdfs://mycluster:9000/library/SparkStreaming/Checkpoint_Data";
      JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
         @Override
         public JavaStreamingContext create() {
            return createContext(checkpointDirectory,conf);
         }
      };
      JavaStreamingContext  context = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);*/

      /*
       * 第三步：创建Spark Streaming输入数据来源input Stream：
       * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
       * 2, 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
       *        的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
       *        应用程序的运行而言，有无数据其处理流程都是一样的)； 
       * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
       *        实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
       * 4,在本案例中具体参数含义:
       *        第一个参数是StreamingContext实例;
       *        第二个参数是ZooKeeper集群信息(接受Kafka数据的时候会从ZooKeeper中获得Offset等元数据信息)
       *        第三个参数是Consumer Group
       *        第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
       */

      /*
       * 创建Kafka元数据,来让Spark Streaming这个Kafka Consumer利用
       */
      Map<String, String> kafkaParameters = new HashMap<String, String>();
      kafkaParameters.put("metadata.broker.list", 
            "Master:9092,Worker1:9092,Worker2:9092");
      
      Set<String> topics =  new HashSet<String>();
      topics.add("AdClicked");
      JavaPairInputDStream<String, String> adClickedStreaming = KafkaUtils.createDirectStream(jsc,
            String.class, String.class, 
            StringDecoder.class, StringDecoder.class,
            kafkaParameters, 
            topics);
      /*
       * 因为要对黑名单进行在线过滤，而数据是在RDD中的，所以必然使用transform这个函数；
       * 但是在这里我们必须使用transformToPair，原因是读取进来的Kafka的数据是Pair<String,String>类型的,另外
       * 一个原因是过滤后的数据要进行进一步处理，所以必须是读进来的Kafka数据的原始类型DStream<String, String>
       *
       * 在此：再次说明每个Batch Duration中实际上讲输入的数据就是被一个且仅仅被一个RDD封装的，你可以有多个
       * InputDstream，但是其实在产生Job的时候，这些不同的InputDstream在Batch Duration中就相当于Spark基于
       * HDFS数据操作的不同文件来源而已罢了。
       */
      JavaPairDStream<String, String> filteredadClickedStreaming = adClickedStreaming.transformToPair(new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

         @Override
         public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
            /*
             * 在线黑名单过滤思路步骤：
             * 1，从数据库中获取黑名单转换成RDD，即新的RDD实例封装黑名单数据；
             * 2，然后把代表黑名单的RDD的实例和Batch Duration产生的rdd进行join操作,准确的说是进行
             * leftOuterJoin操作，也就是说使用Batch Duration产生的rdd和代表黑名单的RDD的实例进行
             * leftOuterJoin操作，如果两者都有内容的话，就会是true，否则的话就是false；
             * 
             * 我们要留下的是leftOuterJoin操作结果为false；
             * 
             */
            
            final List<String> blackListNames = new ArrayList<>();
            JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
            jdbcWrapper.doQuery("SELECT * FROM blacklisttable", null, new ExecuteCallBack(){

               @Override
               public void resultCallBack(ResultSet result) throws Exception {
                  
                  while(result.next()){
                     blackListNames.add(result.getString(1));
                  }
               }
               
            });
            
            List<Tuple2<String, Boolean>> blackListTuple = new ArrayList<Tuple2<String, Boolean>>();
            
            for (String name : blackListNames){
               blackListTuple.add(new Tuple2<String,Boolean>(name, true));
            }
            
            List<Tuple2<String, Boolean>> blackListFromDB = blackListTuple; //数据来自于查询的黑名单表并且映射成为<String, Boolean>
            
            JavaSparkContext jsc = new JavaSparkContext(rdd.context());
            
            /*
             * 黑名单的表中只有userID，但是如果要进行join操作的话，就必须是Key-Value，所以
             * 在这里我们需要基于数据表中的数据产生Key-Value类型的数据集合；
             */
            JavaPairRDD<String, Boolean> blackListRDD = jsc.parallelizePairs(blackListFromDB);
            
               
            /*
             * 进行操作的时候肯定是基于userID进行join的，所以必须把传入的rdd进行mapToPair操作转化成为符合
             * 格式的rdd
             * 
             * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
             */
            
            JavaPairRDD<String, Tuple2<String, String>>  rdd2Pair = rdd.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<String,String>>() {

               @Override
               public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
                  String userID = t._2.split("\t")[2];
                  return new Tuple2<String, Tuple2<String, String>>(userID, t);
               }
            });
            //格式 <userid,   Tuple2<Tuple2< kafkaoffset,userinfo >,boolean >>
             JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joined = rdd2Pair.leftOuterJoin(blackListRDD);
            
             JavaPairRDD<String, String> result = joined.filter(new Function<Tuple2<String,
                   Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {

                     @Override
                     public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> v1)
                           throws Exception {
                        Optional<Boolean> optional = v1._2._2;
                        
                        /*if (optional.isPresent() && optional.get()){
                           return false;
                        } else {
                           return true;
                        }*/
                        return optional.isPresent() ? optional.get() : false;
                        
                     }
            }).mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {

               @Override
               public Tuple2<String, String> call(
                     Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> t) throws Exception {
                  // TODO Auto-generated method stub
                  return t._2._1;
               }
            });
              
         
            return result;
         }
      });
      
      
      /*
       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
       *对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
        *     广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
        */
   
      
      JavaPairDStream<String, Long> pairs = adClickedStreaming.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

         @Override
         public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
            String[] splited = t._2.split("\t");
            
            String timestamp = splited[0]; //yyyy-MM-dd
            String ip = splited[1];
            String userID = splited[2];
            String adID = splited[3];
            String province = splited[4];
            String city = splited[5];
            
            String clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" 
                  + province + "_" + city;
            
            return new Tuple2<String, Long>(clickedRecord, 1L);
         }
      });
      
       /*
          * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
          *   计算每个Batch Duration中每个User的广告点击量
          */
      JavaPairDStream<String, Long> adClickedUsers = pairs.reduceByKey(new Function2<Long, Long, Long>(){

         @Override
         public Long call(Long v1, Long v2) throws Exception {
            // TODO Auto-generated method stub
            return v1 + v2;
         }
            
      });
      
      
      /*
       * 
       * 计算出什么叫有效的点击？
       * 1，复杂化的一般都是采用机器学习训练好模型直接在线进行过滤；
       * 2，简单的？可以通过一个Batch Duration中的点击次数来判断是不是非法广告点击，但是实际上讲非法广告
       * 点击程序会尽可能模拟真实的广告点击行为，所以通过一个Batch来判断是 不完整的，我们需要对例如一天（也可以是每一个小时）
       * 的数据进行判断！
       * 3，比在线机器学习退而求次的做法如下：
       *        例如：一段时间内，同一个IP（MAC地址）有多个用户的帐号访问；
       *        例如：可以统一一天内一个用户点击广告的次数，如果一天点击同样的广告操作50次的话，就列入黑名单；
       * 
       * 黑名单有一个重点的特征：动态生成！！！所以每一个Batch Duration都要考虑是否有新的黑名单加入，此时黑名单需要存储起来
       * 具体存储在什么地方呢，存储在DB/Redis中即可；
       * 
       * 例如邮件系统中的“黑名单”，可以采用Spark Streaming不断的监控每个用户的操作，如果用户发送邮件的频率超过了设定的值，可以
       * 暂时把用户列入“黑名单”，从而阻止用户过度频繁的发送邮件。
       */
       
      JavaPairDStream<String, Long>  filteredClickInBatch = adClickedUsers.filter(new Function<Tuple2<String,Long>, Boolean>() {
         
         @Override
         public Boolean call(Tuple2<String, Long> v1) throws Exception {
            if ( 1 < v1._2){
               //更新一下黑名单的数据表
               return false;
            } else {
               return true;
            }
            
         }
      });
      
      // Todo。。。。
      
      /*
       * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark Streaming
       * 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
       * 
       * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
       * output Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
       * 方法是foraeachRDD,因为Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
       * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
       *
       */
//    filteredClickInBatch.print();
      
      filteredClickInBatch.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

         @Override
         public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
               
               @Override
               public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                  /*
                   * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
                   * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
                   * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
                   * 插入的用户信息可以只包含：timestamp、ip、userID、adID、province、city
                   * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
                   */
                  
                  List<UserAdClicked> userAdClickedList = new ArrayList<UserAdClicked>();
                  
                  while (partition.hasNext()){
                     Tuple2<String, Long> record = partition.next();
                     String[] splited = record._1.split("\t");
                     
                     UserAdClicked userClicked = new UserAdClicked();
                     userClicked.setTimestamp(splited[0]);
                     userClicked.setIp(splited[1]);
                     userClicked.setUserID(splited[2]);
                     userClicked.setAdID(splited[3]);
                     userClicked.setProvince(splited[4]);
                     userClicked.setCity(splited[5]);
                     userAdClickedList.add(userClicked);
                     
                  }
                  
                  final List<UserAdClicked> inserting  = new ArrayList<UserAdClicked>();
                  final List<UserAdClicked> updating  = new ArrayList<UserAdClicked>();
                  
                  JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                  
                  //adclicked 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
                  for (final UserAdClicked clicked : userAdClickedList){
                     jdbcWrapper.doQuery("SELECT count(1) FROM adclicked WHERE "
                           + " timestamp = ? AND userID = ? AND adID = ?",
                           new Object[]{clicked.getTimestamp(), clicked.getUserID(), clicked.getAdID()},
                           new ExecuteCallBack() {
                              
                              @Override
                              public void resultCallBack(ResultSet result) throws Exception {
                                 if(result.next()){
                                    long count = result.getLong(1);
                                    clicked.setClickedCount(count);
                                    updating.add(clicked);
                                 } else {
                                    inserting.add(clicked);
                                 }
                                 
                              }
                           });
                  }
               //adclicked 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
               ArrayList<Object[]> insertParametersList = new ArrayList<Object[]>();
               for(UserAdClicked inserRecord : inserting){
                  insertParametersList.add(new Object[]{
                        inserRecord.getTimestamp(),
                        inserRecord.getIp(),
                        inserRecord.getUserID(),
                        inserRecord.getAdID(),
                        inserRecord.getProvince(),
                        inserRecord.getCity(),
                        inserRecord.getClickedCount()
                  });
               }
               jdbcWrapper.doBatch("INSERT INTO adclicked VALUES(?,?,?,?,?,?,?)", insertParametersList);
               
               
               
               //adclicked 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
               ArrayList<Object[]> updateParametersList = new ArrayList<Object[]>();
               for(UserAdClicked updateRecord : updating){
                  updateParametersList.add(new Object[]{
                        updateRecord.getTimestamp(),
                        updateRecord.getIp(),
                        updateRecord.getUserID(),
                        updateRecord.getAdID(),
                        updateRecord.getProvince(),
                        updateRecord.getCity(),
                        updateRecord.getClickedCount()
                  });
               }
               jdbcWrapper.doBatch("UPDATE adclicked set clickedCount = clickedCount + 1 WHERE "
                           + " timestamp = ? AND userID = ? AND adID = ?", updateParametersList);
               
               
               
               }
            });
            return null;
         }
         
      
      });
      
      
      JavaPairDStream<String, Long> blackListBasedOnHistory = filteredClickInBatch.filter(new Function<Tuple2<String,Long>, Boolean>() {

         @Override
         public Boolean call(Tuple2<String, Long> v1) throws Exception {
            //广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
            String[] splited = v1._1.split("\t");
            
            String date = splited[0];
            String userID = splited[2];
            String adID = splited[3];
            
            /**
             * 接下来根据date、userID、adID等条件去查询用户点击广告的数据表，获得总的点击次数
             * 这个时候基于点击次数判断是否属于黑名单点击             * 
             */
            
            int clickedCountTotalToday = 81;
                  
            if (clickedCountTotalToday > 50){
               return true;
            } else {
               return false;
            }
                     
         }
      });
      
      
      /*
       * 必须对黑名单的整个RDD进行去重操作！！！
       */
      
      
      JavaDStream<String> blackListuserIDtBasedOnHistory = blackListBasedOnHistory.map(new Function<Tuple2<String,Long>, String>() {

         @Override
         public String call(Tuple2<String, Long> v1) throws Exception {
            // TODO Auto-generated method stub
            return v1._1.split("\t")[2];
         }
      });
      
      JavaDStream<String> blackListUniqueuserIDtBasedOnHistory = blackListuserIDtBasedOnHistory.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

         @Override
         public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
            // TODO Auto-generated method stub
            return rdd.distinct();
         }
      });
      
      
      
      //下一步写入黑名单数据表中
      
      blackListUniqueuserIDtBasedOnHistory.foreachRDD(new Function<JavaRDD<String>, Void>() {

         @Override
         public Void call(JavaRDD<String> rdd) throws Exception {
            rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
               
               @Override
               public void call(Iterator<String> t) throws Exception {
                  /**
                   * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
                   * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
                   * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
                   * 插入的用户信息可以只包含：useID
                   * 此时直接插入黑名单数据表即可。
                   */
                  
                  List<Object[]> blackList = new ArrayList<Object[]>();
                  
                  while(t.hasNext()){
                     blackList.add(new Object[]{(Object)t.next()});
                  }
                  JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                  jdbcWrapper.doBatch("INSERT INTO blacklisttable VALUES (?) ", blackList);
               }
            });
            return null;
         }
      });
      
      /*
       * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
       * 接受应用程序本身或者Executor中的消息；
       */
      jsc.start();
      
      jsc.awaitTermination();
      jsc.close();

   }

   private static JavaStreamingContext createContext(String checkpointDirectory,SparkConf conf){
      System.out.println("Creating new context");
      SparkConf  sparkConf = conf;
      JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,Durations.seconds(5));
      ssc.checkpoint(checkpointDirectory);
      return ssc;
   }

}