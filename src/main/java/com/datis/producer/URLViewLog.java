/*
*This class for send every Second a Word to specified topic
*i will with this want test windowing. 
 */
package com.datis.producer;

import com.datis.pojo.entity.URLView;
import com.datis.pojo.serde.kryo.URLVuSerializer;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 *
 * @author jeus
 */
public class URLViewLog extends Thread {

    private final String topic;
    final URLVuSerializer urlViewSerialize = new URLVuSerializer();
    LongSerializer longSerializer = new LongSerializer();
    Properties propr = new Properties();
    String[] regions = "Iran,USA,Afghanistan,Albania,Algeria".split(",");
    String[] users = "saeed,mehdi,majid,arman,mostafa,ali,behnam".split(",");
    String[] urls = "yahoo,pohub,google,piratebay".split(",");
    KafkaProducer<Long, URLView> producer;

    public URLViewLog() {
        topic = "viewlog";
        propr = new Properties();
        propr.put("bootstrap.servers", "172.17.0.13:9092");
        propr.put("client.id", "viewlogProd");
        propr.put("buffer.memory", 33554432);
//        propr.put("batch.size",800);//this for async by size in buffer
//        propr.put("linger.ms", 9000);//this for async by milisecond messages buffered
        propr.put("acks", "1");
        propr.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        propr.put("value.serializer", "com.datis.pojo.serde.kryo.URLVuSerializer");
        producer = new KafkaProducer<>(propr, longSerializer, urlViewSerialize);
    }

    @Override
    public void run() {
        System.out.println("THIS START");
        //send data by sync data to consumer; //if not send Data to topic try again 
        Date dt;
        while (true) {
            dt = new Date();
            Long key = dt.getTime();
            try {
                URLViewCallback regCallBack = new URLViewCallback(dt.getTime(), getWord());
//                RecordMetadata rc =
                producer.send(new ProducerRecord<>(topic, key, getWord()), regCallBack);
//                System.out.println("Send Data To Topic Sync:" + rc.offset() + "   Str:" + rc.toString());

                Thread.sleep(3000);
            } catch (InterruptedException ex) {
                Logger.getLogger(URLViewLog.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (ExecutionException ex) {
//                Logger.getLogger(Region.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String[] args) {
        URLViewLog reg = new URLViewLog();
        reg.start();
    }

    private URLView getWord() {
        Random rnd = new Random();
        URLView urlv = new URLView();
        urlv.setUrl(this.urls[rnd.nextInt(this.urls.length)]);
        urlv.setRegion(this.regions[rnd.nextInt(this.regions.length)]);
        urlv.setUser(this.users[rnd.nextInt(this.users.length)]);
        return urlv;
    }

}

class URLViewCallback implements Callback {

    private final Long key;
    private final URLView urlView;

    public URLViewCallback(long stTime, URLView msg) {
        this.key = stTime;
        this.urlView = msg;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("message(" + key + ", " + urlView.toString() + ") sent to partition(" + metadata.partition()
                    + "), " + "offset(" + metadata.offset() + ")");
        } else {
            exception.printStackTrace();
        }

    }

}
