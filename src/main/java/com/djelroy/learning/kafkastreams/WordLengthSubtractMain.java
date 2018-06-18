package com.djelroy.learning.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class WordLengthSubtractMain {

	private final static String INPUT_TOPIC = "word-length-subtract-input";
	private final static String OUTPUT_TOPIC = "word-length-subtract-output";

	public static void main(String[] args) {

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Integer> intSerde = Serdes.Integer();

		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-length-subtract");
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "word-length-sum-client");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> wordsInput = streamsBuilder.stream(INPUT_TOPIC,
				Consumed.with(stringSerde, stringSerde));

		// Extract the 2 line words into String[] 
		// Make the line (record value )as the new key (for printing purposes)
		wordsInput.map((k, v) -> KeyValue.pair(v, v.split("\\W+")))
			// Subtract the words' lengths
			.mapValues(v -> Math.abs(v[0].length() - v[1].length()))
			// Write to the output topic
			.to(OUTPUT_TOPIC, Produced.with(stringSerde, intSerde));

		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
