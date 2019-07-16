package io.andreidiego.akka.scraper;

import org.jsoup.select.Elements;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

class ImageLinkScraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;

    static Props props() {
        return Props.create(ImageLinkScraper.class);
    }

    public ImageLinkScraper() {
        scraperName = self().path().name();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ScrapperProtocol.ScrapeThis.class, msg -> {
                    final String senderName = sender().path().name();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} scraping url {}. Searching for image links...", scraperName, msg.getScrapeJob().getUrl());

                    final Elements imageLinks = msg.getDocument().select("img[src]");

                    imageLinks.forEach(imageLink -> {
                        final String imageUrl = imageLink.attr("abs:src");

                        log.debug("{}'s found a new image url: {}.", scraperName, imageUrl);
                        log.debug("{} sending the newly found image url ({}) to {}...", scraperName, imageUrl, sender().path().name());

                        //sender().tell(new ScrapperProtocol.NewUrlFound(imageUrl, msg.getScrapeJob().getDepth() + 1), self());
                    });
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, scraperName);
                })
                .build();
    }
}