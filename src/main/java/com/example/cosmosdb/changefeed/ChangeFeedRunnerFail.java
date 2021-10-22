package com.example.cosmosdb.changefeed;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedContextClientImpl;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Slf4j
@RequiredArgsConstructor
public class ChangeFeedRunnerFail {

    @Value("${cosmosdb.database}")
    private String database;

    @Value("${cosmosdb.container}")
    private String container;


    private final CosmosClientFactory cosmosClientFactory;


    public void consumeChangeFeed() {
        CosmosAsyncClient asyncClient = cosmosClientFactory.createAsyncClient();
        CosmosAsyncContainer asyncContainer = asyncClient.getDatabase(database).getContainer(container);

        ChangeFeedContextClientImpl changeFeedContextClient = new ChangeFeedContextClientImpl(asyncContainer);
        CosmosChangeFeedRequestOptions changeFeedOptions = ChangeFeedUtil.createChangeFeedRequestOptions(null, null, 100);
        ChangeFeedUtil.DocumentsWithContinuationToken documentsWithContinuationToken = changeFeedContextClient.createDocumentChangeFeedQuery(asyncContainer, changeFeedOptions)
                .doOnNext(this::logFeedResponseMetadata)
                .take(10)
                .collectList()
                .map(ChangeFeedUtil::mapFeedResponses)
                .blockOptional(Duration.ofSeconds(30))
                .orElseThrow(() -> new IllegalStateException("Response from change feed is empty even after polling multiple times"));

        log.info("Read {} items from changefeed. Next continuation token is: {}", documentsWithContinuationToken.getDocuments().size(), documentsWithContinuationToken.getContinuationToken());
    }

    private void logFeedResponseMetadata(FeedResponse<JsonNode> feedResponse) {
        log.info("Queried change feed: RU usage: {}, result size: {}, result continuation-token: {}", feedResponse.getRequestCharge(), feedResponse.getResults().size(), feedResponse.getContinuationToken());
    }
}
