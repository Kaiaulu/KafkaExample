package be.kaiaulu;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class KafkaWeatherFeed {

    private static final String BOOTSTRAP_SERVERS = "192.168.4.87:32775";
    private static final String TOPIC = "kafka-example";

    private static void getWeather() {
        try {
            URL weatherURL = new URL("http://w1.weather.gov/xml/current_obs/all_xml.zip");

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");

            Producer<String, String> producer = new KafkaProducer<>(props);
            URLConnection conn = weatherURL.openConnection();
            ZipInputStream zis = new ZipInputStream(conn.getInputStream());
            ZipEntry ze = null;
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.getName().indexOf(".") != 4)
                    continue;
                StringWriter writer = new StringWriter();
                IOUtils.copy(zis, writer);
                String weatherReport = writer.toString();
                ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, weatherReport);
                producer.send(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        getWeather();
    }

}
