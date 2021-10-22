package com.example.cosmosdb.changefeed;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
@RequiredArgsConstructor
public class ChangeFeedRunnerWorks {

    @Value("${cosmosdb.database}")
    private String database;

    @Value("${cosmosdb.container}")
    private String container;


    private final CosmosClientFactory cosmosClientFactory;

    public void consumeChangeFeed() {
        CosmosAsyncClient asyncClient = cosmosClientFactory.createAsyncClient();
        CosmosAsyncContainer asyncContainer = asyncClient.getDatabase(database).getContainer(container);


        CosmosChangeFeedRequestOptions changeFeedOptions = ChangeFeedUtil.createChangeFeedRequestOptions(null, null, 100);
        CosmosPagedFlux<JsonNode> pagedFlux = asyncContainer.queryChangeFeed(changeFeedOptions, JsonNode.class);
        Flux<FeedResponse<JsonNode>> feedResponseFlux = pagedFlux.byPage();
        ChangeFeedUtil.DocumentsWithContinuationToken documentsWithContinuationToken = feedResponseFlux
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
