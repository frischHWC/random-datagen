package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

/**
 * This is a Kafka Sink
 */
public class KafkaSink implements SinkInterface {

    private Producer<String, GenericRecord> producer;
    private Producer<String, String> producerString;
    private String topic;
    private Schema schema;
    private MessageType messagetype;

    public void init(Model model) {

        schema = model.getAvroSchema();
        this.messagetype = convertStringToMessageType((String) model.getOptions().get(OptionsConverter.Options.KAFKA_MESSAGES_TYPE));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesLoader.getProperty("kafka.brokers"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if(messagetype==MessageType.AVRO) {
            String schemaRegistryProtocol = "http";
          // SSL configs
            if (Boolean.parseBoolean(PropertiesLoader.getProperty("schema.registry.tls"))) {
                System.setProperty("javax.net.ssl.trustStore",
                    PropertiesLoader.getProperty("kafka.truststore.location"));
                System.setProperty("javax.net.ssl.trustStorePassword",
                    PropertiesLoader.getProperty("kafka.truststore.password"));
                System.setProperty("javax.net.ssl.keyStore",
                    PropertiesLoader.getProperty("kafka.keystore.location"));
                System.setProperty("javax.net.ssl.keyStorePassword",
                    PropertiesLoader.getProperty("kafka.keystore.pasword"));
                schemaRegistryProtocol = "https";
            }

            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer");
            props.put(
                SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(),
                schemaRegistryProtocol + "://" + PropertiesLoader.getProperty("schema.registry.url") + "/api/v1");
            props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

        } else {
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        }

        String securityProtocol = PropertiesLoader.getProperty("kafka.security.protocol");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        //Kerberos config
        if (securityProtocol.equalsIgnoreCase("SASL_PLAINTEXT") || securityProtocol.equalsIgnoreCase("SASL_SSL")) {
            Utils.createJaasConfigFile("kafka-jaas-randomdatagen.config", "KafkaClient",
                    PropertiesLoader.getProperty("kafka.auth.kerberos.keytab"), PropertiesLoader.getProperty("kafka.auth.kerberos.user"),
                    true, false, false);
            Utils.createJaasConfigFile("kafka-jaas-randomdatagen.config", "RegistryClient",
                    PropertiesLoader.getProperty("kafka.auth.kerberos.keytab"), PropertiesLoader.getProperty("kafka.auth.kerberos.user"),
                    true, false, true);
            System.setProperty("java.security.auth.login.config", "kafka-jaas-randomdatagen.config");

            props.put(SaslConfigs.SASL_MECHANISM, PropertiesLoader.getProperty("kafka.sasl.mechanism"));
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, PropertiesLoader.getProperty("kafka.sasl.kerberos.service.name"));

            Utils.loginUserWithKerberos(PropertiesLoader.getProperty("kafka.auth.kerberos.user"),
                    PropertiesLoader.getProperty("kafka.auth.kerberos.keytab"), new Configuration());
        }

        // SSL configs
        if (securityProtocol.equalsIgnoreCase("SASL_SSL") || securityProtocol.equalsIgnoreCase("SSL")) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, PropertiesLoader.getProperty("kafka.keystore.location"));
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, PropertiesLoader.getProperty("kafka.truststore.location"));
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, PropertiesLoader.getProperty("kafka.keystore.key.password"));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PropertiesLoader.getProperty("kafka.keystore.pasword"));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PropertiesLoader.getProperty("kafka.truststore.password"));
        }

        topic = (String) model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TOPIC);

        if(messagetype==MessageType.AVRO) {
            producer = new KafkaProducer<>(props);
        } else {
            producerString = new KafkaProducer<>(props);
        }
    }

    public void terminate() {
        if(messagetype==MessageType.AVRO) {
            producer.close();
        } else {
            producerString.close();
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        ConcurrentLinkedQueue<Future<RecordMetadata>> queue = new ConcurrentLinkedQueue<>();
        if(messagetype==MessageType.AVRO) {
            rows.parallelStream()
                .map(row -> row.toKafkaMessage(schema))
                .forEach(keyValue ->
                    queue.add(
                        producer.send(
                            new ProducerRecord<>(
                                topic,
                                (String) keyValue.getKey(),
                                (GenericRecord) keyValue.getValue()
                            )
                        ))
                );
        } else {
            rows.parallelStream()
                .map(row -> row.toKafkaMessageString(messagetype))
                .forEach(keyValue ->
                    queue.add(
                        producerString.send(
                            new ProducerRecord<>(
                                topic,
                                (String) keyValue.getKey(),
                                (String) keyValue.getValue()
                            )
                        ))
                );
        }
        checkMessagesHaveBeenSent(queue);
    }

    /**
     * Goal is to not overflow kafka brokers and verify they well received data sent before going further
     *
     * @param queue
     * @return
     */
    private void checkMessagesHaveBeenSent(ConcurrentLinkedQueue<Future<RecordMetadata>> queue) {
        while (!queue.isEmpty()) {
            Future<RecordMetadata> metadata = queue.poll();
            if (!metadata.isDone()) {
                queue.add(metadata);
            }
        }
    }

    public enum MessageType {
        AVRO,
        CSV,
        JSON
    }

    private MessageType convertStringToMessageType(String messageType) {
        switch (messageType.toUpperCase()) {
        case "AVRO": return MessageType.AVRO;
        case "CSV": return MessageType.CSV;
        case "JSON":
        default: return MessageType.JSON;
        }
    }
}
