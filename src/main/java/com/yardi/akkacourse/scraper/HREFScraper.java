package com.yardi.akkacourse.scraper;

import com.yardi.akkacourse.ScrapeJob;

import org.jsoup.select.Elements;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

class HREFScraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;

    static Props props() {
        return Props.create(HREFScraper.class);
    }

    public HREFScraper() {
        scraperName = self().path().name();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ScrapperProtocol.ScrapeThis.class, msg -> {
                    final String senderName = sender().path().name();
                    final ScrapeJob scrapeJob = msg.getScrapeJob();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} scraping url {}. Searching for href links...", scraperName, scrapeJob.getUrl());

                    final Elements links = msg.getDocument().select("a[href]");

                    links.forEach(hrefLink -> {
                        final String url = hrefLink.attr("abs:href");

                        log.debug("{}'s found a new url: {}.", scraperName, url);
                        log.debug("{} sending the newly found url ({}) to {}...", scraperName, url, sender().path().name());

                        sender().tell(new ScrapperProtocol.NewUrlFound(url, scrapeJob), self());
                    });
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, scraperName);
                })
                .build();
    }
}