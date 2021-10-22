package com.example.cosmosdb.changefeed;

import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.FeedResponse;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class ChangeFeedUtil {

    public static CosmosChangeFeedRequestOptions createChangeFeedRequestOptions(@Nullable Instant from, @Nullable String continuationToken, int maxItemCount) {
        CosmosChangeFeedRequestOptions changeFeedOptions;
        if (from == null && continuationToken == null) {
            changeFeedOptions = CosmosChangeFeedRequestOptions.createForProcessingFromBeginning(FeedRange.forFullRange());
        } else if (continuationToken == null) {
            changeFeedOptions = CosmosChangeFeedRequestOptions.createForProcessingFromPointInTime(from, FeedRange.forFullRange());
        } else {
            if (from != null) {
                log.warn("Ignoring 'from' {} as continuation token was also provided", from);
            }
            changeFeedOptions = CosmosChangeFeedRequestOptions.createForProcessingFromContinuation(continuationToken);
        }
        changeFeedOptions.setMaxItemCount(maxItemCount);
        return changeFeedOptions;
    }

    public static DocumentsWithContinuationToken mapFeedResponses(List<FeedResponse<JsonNode>> feedResponses) {
        var documents = feedResponses.stream()
                .flatMap(feedResponse -> feedResponse.getResults().stream())
                .collect(Collectors.toList());

        // find the "last" continuation token
        var continuationToken = feedResponses.stream()
                // pick last continuation token
                .reduce((a, b) -> b)
                .map(FeedResponse::getContinuationToken)
                .orElseThrow(() -> new IllegalStateException("Cannot find continuation token from change feed events"));

        return DocumentsWithContinuationToken.of(continuationToken, documents);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE, staticName = "of")
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class DocumentsWithContinuationToken {
        private final String continuationToken;
        private final List<JsonNode> documents;
    }

}
