/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.kryo;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import java.io.ByteArrayOutputStream;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 *
 * @author jeus
 */
public class KryoSerializer<T> implements Serializer<T> {

    public KryoSerializer() {

    }
 private Class<T> tClass;
    
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {

    }

    @Override
    public byte[] serialize(String arg0, T data) {
        Kryo kryo = new Kryo();
        FieldSerializer<?> serializer = new FieldSerializer<T>(kryo, tClass);
        kryo.register(tClass, serializer);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        if (data != null) {
            Output output = new Output(stream);
            kryo.writeObject(output,(Class<T>) data);
            output.close();
        }
        return stream.toByteArray();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
