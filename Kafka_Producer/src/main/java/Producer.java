import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class Producer {

    private static Producer producer;

    private KafkaProducer<String, String> kafkaProducer;



    private Producer() {

        try {
            var conf = new Properties();
            conf.load(new FileReader("src\\main\\resources\\producer.properties"));
            kafkaProducer = new KafkaProducer<>(conf);
        }catch (IOException e){
            log.error(e.getMessage());
        }

    }

    public void send(String key, String value){

        try {
//            Properties properties = new Properties();
//            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            kafkaProducer = new KafkaProducer<>(properties);
            var record = new ProducerRecord<String, String>(TOPIC, PARTITION, key, value);
            this.kafkaProducer.send(record);

        }catch (KafkaException e){
            log.error(e.getMessage());
            this.close();
        }

    }

    public void close(){

        this.kafkaProducer.close();

    }

    public static Producer getInstance(){
        return (Objects.nonNull(producer))? producer : new Producer();
    }
    private static final String TOPIC = "topico-ejemplo";
    private static final Integer PARTITION = 0;
    private static final Logger log = LogManager.getLogger(Producer.class);

}
