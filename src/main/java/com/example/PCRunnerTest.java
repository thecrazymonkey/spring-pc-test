package com.example;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;


public class PCRunnerTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(PCRunnerTest.class);

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "pcGroup");
		props.put("enable.auto.commit", false);
		props.put("isolation.level", "read_committed");
		props.put("max.poll.interval.ms", 60000);
		var options = ParallelConsumerOptions.<String, String>builder()
				.ordering(KEY)
				.maxConcurrency(10)
				.consumer(new KafkaConsumer<>(props))
				.build();
		ParallelStreamProcessor<String, String> eosStreamProcessor =
				ParallelStreamProcessor.createEosStreamProcessor(options);
		eosStreamProcessor.subscribe(of("topic2"));
		eosStreamProcessor.poll(recordTest -> {
			ConsumerRecord<String,String> rec = recordTest.getSingleRecord().getConsumerRecord();
			LOGGER.info("Concurrently processing a record: {}", rec);
			try {
				Thread.sleep(70000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOGGER.info("Acking: {}", rec.key());
		});
	}
}
