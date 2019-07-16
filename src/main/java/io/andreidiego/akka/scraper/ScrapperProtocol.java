package io.andreidiego.akka.scraper;

import io.andreidiego.akka.ScrapeJob;

import org.jsoup.nodes.Document;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Wither;

public class ScrapperProtocol {

    private interface Command {
    }

    @Value
    @Builder
    public static class ScrapeThis implements Command {
        private final ScrapeJob scrapeJob;
        @Wither
        private final Document document;
        @Wither
        private final String wordToFind;
    }

    private interface Event {
    }

    @Value
    public static class NewUrlFound implements Event {
        private final String newURL;
        private final ScrapeJob originalScrapeJob;
    }

    @Value
    public static class ScrapeFailed implements Event {
        private final ScrapeJob scrapeJob;
    }
}