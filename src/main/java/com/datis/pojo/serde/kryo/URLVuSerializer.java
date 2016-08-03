/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.serde.kryo;


import com.datis.pojo.entity.URLView;
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
public class URLVuSerializer implements Serializer<URLView> {

    public URLVuSerializer() {

    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {

    }

    @Override
    public byte[] serialize(String arg0, URLView data) {
        Kryo kryo = new Kryo();
        FieldSerializer<?> serializer = new FieldSerializer<URLView>(kryo, URLView.class);
        kryo.register(URLView.class, serializer);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        if (data != null) {
            Output output = new Output(stream);
            kryo.writeObject(output, (URLView) data);
            output.close();
        }
        return stream.toByteArray();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
