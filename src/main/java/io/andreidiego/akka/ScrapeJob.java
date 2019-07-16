package io.andreidiego.akka;

import java.io.Serializable;

import lombok.Value;

@Value
public class ScrapeJob implements Serializable {
    private final String url;
    final int failureCount;
    private final int depth;
}