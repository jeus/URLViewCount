/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.serde.kryo;

import com.datis.pojo.entity.URLView;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author jeus
 */
public class URLVuDeserializer implements Deserializer<URLView> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private URLView tClass;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean arg1) {

    }

    @Override
    public URLView deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        URLView data;
        try {
            Kryo kryo = new Kryo();
            Input input = new Input(bytes);
            data = kryo.readObject(input, URLView.class);
            input.close();
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return data;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
