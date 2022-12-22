package com.kafka.java.demo.opensearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearClient() {
        String connString = "http://localhost:9200";
        // String connString =
        // "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))

                            .setHttpClientConfigCallback((HttpClientConfigCallback) getBuilder(cp)));

        }

        return restHighLevelClient;
    }

    public static HttpAsyncClientBuilder getBuilder(CredentialsProvider cp) {
        return HttpAsyncClientBuilder.create().setDefaultCredentialsProvider(cp)
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy());
    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

        // create opensearch client

        RestHighLevelClient openSearClient = createOpenSearClient();

        // create kafka client
        KafkaConsumer<String, String> consumer = getKafkaConsumer();

        // create index in open search database

        try {

            Boolean isExists = openSearClient.indices().exists(new GetIndexRequest("wikimedia"),
                    RequestOptions.DEFAULT);
            if (!isExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                System.out.println("index create successfully");
            } else {
                System.out.println("Index created already");
            }

            // kakfa consumer

            consumer.subscribe(Collections.singleton("wikimedia_demo"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord record : records) {

                    try {
                        // Send the index into opensearch
                        String id = extractId((String) record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia_another")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
                        // IndexResponse response = openSearClient.index(indexRequest,
                        // RequestOptions.DEFAULT);

                        // log.info("Inserted 1 document into OpenSearch" + response.getId());
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s)");
                }

            }

        } finally {
            openSearClient.close();
            consumer.close();
        }

    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        // props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create kafka consumer config

        return new KafkaConsumer<>(props);
    }

}
