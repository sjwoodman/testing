package com.redhat.demo.clnr.rest;

import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class KstreamT est {

    Logger logger = LoggerFactory.getLogger(KstreamTest.class);


    @KafkaStream(input = "ingest.api.out", output = "kstream.test.out")
    public KStream tableTransformer(final KStream<String, String> source) {

        logger.info("Initial setup");

        final KStream<String, Long> successCountsPerJob = source.filter((key, value) -> {
            System.out.println("x");
            return value.equals("Success");
        })
                .groupByKey()
                .count("successMessagesStore").toStream();

        return successCountsPerJob;
    }
}
