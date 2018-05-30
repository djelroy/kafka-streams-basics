package com.djelroy.learning.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class WordLengthCountMain {

	public static void main(String[] args) {

		final String inputTopic = "streams-words-input";
		final String outputTopic = "streams-words-length-output";

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Integer> intSerde = Serdes.Integer();
		
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-length-counter");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> wordsInput = streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));
		KStream<String, Integer> wordsLength = wordsInput.mapValues(String::length);
		wordsLength.to(outputTopic, Produced.with(stringSerde, intSerde));


		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
