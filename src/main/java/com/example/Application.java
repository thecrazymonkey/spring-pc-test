/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;

import io.confluent.parallelconsumer.ParallelStreamProcessor;

/**
 * Sample showing a batch listener and transactions.
 *
 * @author Gary Russell
 * @since 2.2.1
 *
 */
@SpringBootApplication
public class Application {

	private final Logger LOGGER = LoggerFactory.getLogger(Application.class);
	ParallelStreamProcessor<String, String> eosStreamProcessor;
	@EventListener
	public void executeOnStartup(ApplicationStartedEvent event) {
		LOGGER.info("Initializing PC");
		PCRunner pcr = new PCRunner();
		pcr.pollPC(eosStreamProcessor = pcr.instantiatePC());
	}
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(Application.class, args).close();
	}
	
	@Bean
	@Profile("default") // Don't run from test(s)
	public ApplicationRunner runner(KafkaListenerEndpointRegistry registry) {
		return args -> {
			System.out.println("Hit Enter to terminate...");
			System.in.read();
			eosStreamProcessor.close();
		};
		
	}
	
	@Bean
	public NewTopic topic2() {
		return TopicBuilder.name("topic2").partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topic3() {
		return TopicBuilder.name("topic3").partitions(1).replicas(1).build();
	}

}
