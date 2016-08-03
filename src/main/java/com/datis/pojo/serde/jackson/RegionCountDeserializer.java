/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.serde.jackson;

/**
 *com.datis.irc.pojo.RegionCountDeserializer
 * @author jeus
 */

import com.datis.pojo.entity.RegionCount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;



public class RegionCountDeserializer implements Deserializer<RegionCount> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private RegionCount tClass;

    /**
     * Default constructor needed by Kafka
     */
    public RegionCountDeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (RegionCount) props.get("JsonPOJOClass");
    }

    @Override
    public RegionCount deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        RegionCount data;
        try {
            data = objectMapper.readValue(bytes, RegionCount.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}