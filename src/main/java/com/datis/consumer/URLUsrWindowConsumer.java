/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.consumer;

import com.datis.pojo.entity.RegionCount;
import com.datis.pojo.entity.WindowedUrlUser;
import com.datis.pojo.kryo.KryoDesrializer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;

/**
 * start step3 topic that save windowing class POJO
 *
 * @author jeus
 */
public class URLUsrWindowConsumer extends Thread {

    LongDeserializer longDeserializer = new LongDeserializer();
    KryoDesrializer<WindowedUrlUser> windowedDes;
    KryoDesrializer<RegionCount> regionCountDes;
    private boolean logOn = true;
    List<TopicPartition> tp = new ArrayList<>(4);

    
    KafkaConsumer viewLog;
    KafkaConsumer countURL;
    KafkaConsumer countRegion;
    KafkaConsumer countUser;
    static Properties pro = new Properties();
    String topic = "";

    public URLUsrWindowConsumer() {
    }

    public URLUsrWindowConsumer(List<Properties> props, boolean logOn) {
        this.logOn = logOn;
        Map<String, Object> desPropsKey = new HashMap<>();
        desPropsKey.put("Kryo",WindowedUrlUser.class);
        windowedDes = new KryoDesrializer<>();
        windowedDes.configure(desPropsKey, true);
        
        Map<String, Object> desPropsVal = new HashMap<>();
        desPropsVal.put("Kryo",RegionCount.class);
        regionCountDes= new KryoDesrializer<>();
        regionCountDes.configure(desPropsVal, true);
        
        viewLog = new KafkaConsumer(props.get(0), windowedDes,regionCountDes);
//        countURL = new KafkaConsumer(props.get(1));
//        countRegion = new KafkaConsumer(props.get(2));
//        countUser = new KafkaConsumer(props.get(3));
        this.topic = topic = props.get(0).getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        System.out.println("Change It Is work ---------------------*********" + topic);
        tp.add(new TopicPartition(topic, 0));

    }

    @Override
    public void run() {
        viewLog.subscribe(Collections.singleton(topic));//subscribe all topics for poll
        SimpleDateFormat dt = new SimpleDateFormat("hh:mm:ss");

        viewLog.poll(100);
//        countURL.poll(100);
//        countRegion.poll(100);
//        countUser.poll(100);

        viewLog.seek(new TopicPartition(topic, 0), 1L);
        int position = 8000;
        while (true) {
            StringBuilder strBuilder = new StringBuilder();
            ConsumerRecords<WindowedUrlUser, RegionCount> ViewLogRecords = viewLog.poll(1000);
//            ConsumerRecords<WindowedPageViewByRegion, RegionCount> countURLRecords = viewLog.poll(1000);
//            ConsumerRecords<WindowedPageViewByRegion, RegionCount> countRegionRecords = viewLog.poll(1000);
//            ConsumerRecords<WindowedPageViewByRegion, RegionCount> countUserRecords = viewLog.poll(1000);

//            for (ConsumerRecord<String , String> rec : records) {
            if (ViewLogRecords.count() != 0) {
                if (logOn) {
                    for (ConsumerRecord<WindowedUrlUser, RegionCount> rec : ViewLogRecords) {
                        strBuilder.append("ofs:").append(rec.offset());
                        strBuilder.append(" url:").append(rec.key().url);
                        strBuilder.append(" usr:").append(rec.key().User);
                        strBuilder.append(" time:").append(dt.format(new Date(rec.key().windowStart)));
                        strBuilder.append(" Cnt:").append(rec.value().count);
                        strBuilder.append("\n");
                    }
                }
            }
            System.out.print(strBuilder.toString());
        }
    }

    public static void main(String[] arg) {

        List<Properties> props = new ArrayList<>();
        Properties viewLog = new Properties();
        viewLog.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
        viewLog.put(ConsumerConfig.GROUP_ID_CONFIG, "urlusrwindow");
//        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (4000 * 10000) + "");//change this for increase and decrease packet fethe by viewLog every message is 100Byte
        viewLog.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        viewLog.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        viewLog.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        viewLog.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "latest");
        viewLog.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        viewLog.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
//        viewLog.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.datis.pojo.serde.kryo.URLVuDeserializer");
//        viewLog.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        viewLog.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        props.add(viewLog);

//        Properties propsByUrl = new Properties();
//        propsByUrl.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
//        propsByUrl.put(ConsumerConfig.GROUP_ID_CONFIG, "countURL");
////        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (4000 * 10000) + "");//change this for increase and decrease packet fethe by viewLog every message is 100Byte
//        propsByUrl.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        propsByUrl.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        propsByUrl.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        propsByUrl.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
////        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "latest");
//        propsByUrl.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        propsByUrl.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "com.datis.irc.pojo.WindowDeserializer");
//        propsByUrl.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.datis.pojo.serde.kryo.URLVuDeserializer");
//        props.add(propsByUrl);
//
//        Properties propByRegion = new Properties();
//        propByRegion.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
//        propByRegion.put(ConsumerConfig.GROUP_ID_CONFIG, "countRegion");
////        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (4000 * 10000) + "");//change this for increase and decrease packet fethe by viewLog every message is 100Byte
//        propByRegion.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        propByRegion.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        propByRegion.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        propByRegion.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
////        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "latest");
//        propByRegion.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        propByRegion.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "com.datis.irc.pojo.WindowDeserializer");
//        propByRegion.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.datis.irc.pojo.RegionCountDeserializer");
//        props.add(propByRegion);
//
//        Properties propsByUser = new Properties();
//        propsByUser.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
//        propsByUser.put(ConsumerConfig.GROUP_ID_CONFIG, "countUser");
////        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (4000 * 10000) + "");//change this for increase and decrease packet fethe by viewLog every message is 100Byte
//        propsByUser.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        propsByUser.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        propsByUser.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        propsByUser.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
////        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "latest");
//        propsByUser.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        propsByUser.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "com.datis.irc.pojo.WindowDeserializer");
//        propsByUser.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.datis.irc.pojo.RegionCountDeserializer");
//        props.add(propsByUser);
        URLUsrWindowConsumer consumer = new URLUsrWindowConsumer(props, true);
        consumer.start();
    }

//    import com.datis.pojo.serde.kryo.URLVuDeserializer
//    import org.apache.kafka.common.serialization.LongDeserializer
//    import org.apache.kafka.common.serialization.ByteArrayDeserializer
}
