package com.djelroy.learning.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordLengthSumMain {

	public static void main(String[] args) {
		
		final String inputTopic = "streams-words-input";
		final String outputTopic = "streams-word-length-sum-output";

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Integer> intSerde = Serdes.Integer();
		
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-length-sum");
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "word-length-sum-client");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> wordsInput = streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));
		KTable<Integer, Integer> wordsLength = wordsInput
				.mapValues(String::length)
				.selectKey((k, v) -> 1).groupByKey().reduce((a, b) -> a + b);
		wordsLength.toStream().to(outputTopic, Produced.with(intSerde, intSerde));


		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

		// Don't use this in prod
		streams.cleanUp();		
		streams.start();
				
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
