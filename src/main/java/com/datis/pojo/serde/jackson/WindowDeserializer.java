/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.serde.jackson;

/**
 *com.datis.irc.pojo.WindowDeserializer
 * @author jeus
 */

import com.datis.pojo.entity.WindowedPageViewByRegion;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;



public class WindowDeserializer implements Deserializer<WindowedPageViewByRegion> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private WindowedPageViewByRegion tClass;

    /**
     * Default constructor needed by Kafka
     */
    public WindowDeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (WindowedPageViewByRegion) props.get("JsonPOJOClass");
    }

    @Override
    public WindowedPageViewByRegion deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        WindowedPageViewByRegion data;
        try {
            data = objectMapper.readValue(bytes, WindowedPageViewByRegion.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}