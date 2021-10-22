package com.example.cosmosdb.changefeed;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CosmosClientFactory {

    @Value("${cosmosdb.endpoint}")
    private String endpoint;

    @Value("${cosmosdb.key}")
    private String key;


    public CosmosAsyncClient createAsyncClient() {
        CosmosClientBuilder cosmosClientBuilder = setupCosmosClientBuilder();
        return cosmosClientBuilder.buildAsyncClient();
    }

    private CosmosClientBuilder setupCosmosClientBuilder() {
        GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();
        ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
        return new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .gatewayMode(gatewayConnectionConfig)
                .throttlingRetryOptions(retryOptions)
                .consistencyLevel(ConsistencyLevel.SESSION);
    }


}
