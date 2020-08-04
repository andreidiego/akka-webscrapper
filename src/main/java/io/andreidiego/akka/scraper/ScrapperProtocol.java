package io.andreidiego.akka.scraper;

import io.andreidiego.akka.ScrapeJob;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.jsoup.nodes.Document;

public class ScrapperProtocol {

    private interface Command {
    }

    @Value
    @Builder
    public static class ScrapeThis implements Command {
        ScrapeJob scrapeJob;
        @With
        Document document;
        @With
        String wordToFind;
    }

    private interface Event {
    }

    @Value
    public static class NewUrlFound implements Event {
        String newURL;
        ScrapeJob originalScrapeJob;
    }

    @Value
    public static class ScrapeFailed implements Event {
        ScrapeJob scrapeJob;
    }
}