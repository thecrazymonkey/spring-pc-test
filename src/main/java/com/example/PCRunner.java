package com.example;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;

@EnableAsync
public class PCRunner {
	private final Logger LOGGER = LoggerFactory.getLogger(PCRunner.class);

    public ParallelStreamProcessor<String, String> instantiatePC() {
        var options = ParallelConsumerOptions.<String, String>builder()
            .ordering(KEY) 
            .maxConcurrency(10) 
            .consumer(getKafkaConsumer())
            .build();
        ParallelStreamProcessor<String, String> eosStreamProcessor =
              ParallelStreamProcessor.createEosStreamProcessor(options);
        eosStreamProcessor.subscribe(of("topic2", "topic3")); 
        return eosStreamProcessor;
    }


	Consumer<String, String> getKafkaConsumer() {
		Properties props = new Properties();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "pcGroup");
		props.put("enable.auto.commit", false);
		props.put("isolation.level", "read_committed");
        props.put("max.poll.interval.ms", 60000);
		return new KafkaConsumer<>(props);
	}
    @Async
    public void pollPC(ParallelStreamProcessor<String, String> eosStreamProcessor) {
        eosStreamProcessor.poll(record -> {
            ConsumerRecord<String,String> rec = record.getSingleRecord().getConsumerRecord();
            LOGGER.info("Concurrently processing a record: {}", rec);
//            if (rec.key().equals("bar3")) {
                try {
                    Thread.sleep(70000);
                    
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
//            }
            LOGGER.info("Acking: {}", rec.key());
        });
    }
}
