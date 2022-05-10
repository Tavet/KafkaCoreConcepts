package com.github.tavet.kafka.opensearch;

import java.net.URI;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

public final class OpensearchConnection {
    private static OpensearchConnection instance;
    private RestHighLevelClient client;

    private OpensearchConnection() {
        client = createOpensearchClient();
    }

    public static OpensearchConnection getInstance() {
        if (instance == null) {
            instance = new OpensearchConnection();
        }
        return instance;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    private RestHighLevelClient createOpensearchClient() {
        final String opensearch = "http://localhost:9200";
        RestHighLevelClient client;
        URI opensearchUri = URI.create(opensearch);
        String userInfo = opensearchUri.getUserInfo();

        if (userInfo == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(opensearchUri.getHost(), opensearchUri.getPort(), "http")));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            client = new RestHighLevelClient(RestClient
                    .builder(new HttpHost(opensearchUri.getHost(), opensearchUri.getPort(),
                            opensearchUri.getScheme()))
                    .setHttpClientConfigCallback(
                            asyncBuilder -> asyncBuilder.setDefaultCredentialsProvider(provider)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return client;
    }
}
