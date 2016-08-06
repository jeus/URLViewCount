/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.streaming;

import com.datis.pojo.entity.RegionCount;
import com.datis.pojo.entity.URLView;
import com.datis.pojo.entity.WindowedUrl;
import com.datis.pojo.kryo.KryoDesrializer;
import com.datis.pojo.kryo.KryoSerializer;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 *
 * @author jeus
 */
public class URLCountWindowing {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    public static void main(String[] arg) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "URLWindowing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.11:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<RegionCount> regionCountSerializer = new KryoSerializer<>();
        serdeProps.put("Kryo", RegionCount.class);
        regionCountSerializer.configure(serdeProps, false);
        final Deserializer<RegionCount> regionCountDeserializer = new KryoDesrializer<>();
        serdeProps.put("Kryo", RegionCount.class);
        regionCountDeserializer.configure(serdeProps, false);
        final Serde<RegionCount> regionCountSerde = Serdes.serdeFrom(regionCountSerializer, regionCountDeserializer);

        final KryoSerializer<WindowedUrl> WindowedUrlSerializer = new KryoSerializer<>();
        serdeProps.put("Kryo", WindowedUrl.class);
        WindowedUrlSerializer.configure(serdeProps, false);
        final KryoDesrializer<WindowedUrl> WindowedUrlDeserializer = new KryoDesrializer<>();
        serdeProps.put("Kryo", WindowedUrl.class);
        WindowedUrlDeserializer.configure(serdeProps, false);
        final Serde<WindowedUrl> wPageViewByRegionSerde = Serdes.serdeFrom(WindowedUrlSerializer, WindowedUrlDeserializer);

      
        
        Map<String, Object> serdePropsMap = new HashMap<>();
        serdePropsMap.put("Kryo",URLView.class);
        final KryoSerializer<URLView> serialUrlView = new KryoSerializer<URLView>();
        serialUrlView.configure(serdePropsMap, true);
        final KryoDesrializer<URLView> desrialUrlView = new KryoDesrializer<>();
        desrialUrlView.configure(serdePropsMap, true);
        final Serde<URLView> urlViewSerde = Serdes.serdeFrom(serialUrlView,desrialUrlView);
        
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, URLView> source = builder.stream(longSerde, urlViewSerde, "viewlog");
        System.out.println("Source :" + source.toString());

        KStream<WindowedUrl, RegionCount> counts = 
               source.map((Long key, URLView value) -> new KeyValue<String, URLView>(value.getUrl(), value)).
                //                countByKey("count");
                countByKey(TimeWindows.of("GeoPageViewsWindow", 60 * 1000L).advanceBy(60 * 1000L), Serdes.String())
                .toStream().map(new KeyValueMapper<Windowed<String>, Long, KeyValue<WindowedUrl, RegionCount>>() {
                    @Override
                    public KeyValue<WindowedUrl, RegionCount> apply(Windowed<String> key, Long value) {
                        WindowedUrl wUrl = new WindowedUrl();
                        wUrl.windowStart = key.window().start();
                        wUrl.url = key.key();
                        RegionCount rCount = new RegionCount();
                        rCount.url = key.key();
                        rCount.count = value;

                        return new KeyValue<>(wUrl, rCount);
                    }
                });

        counts.to(wPageViewByRegionSerde, regionCountSerde, "urlwindow");
//        counts.to(Serdes.String(), Serdes.Long(), "step3");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

    }

}
