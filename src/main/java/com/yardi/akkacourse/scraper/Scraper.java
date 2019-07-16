package com.yardi.akkacourse.scraper;

import com.yardi.akkacourse.ScrapeJob;
import com.yardi.akkacourse.coordinator.CoordinatorProtocol;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.URL;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import static java.lang.String.format;

public class Scraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;
    private final ActorRef coordinator;
    private final ActorRef hrefScraper;
    private final ActorRef imageLinkScraper;
    private final ActorRef hashedBodyScraper;
    private final ActorRef wordFinderScraper;

    public static Props props(final ActorRef coordinator) {
        return Props.create(Scraper.class, coordinator);
    }

    public Scraper(final ActorRef coordinator) {
        scraperName = self().path().name();

        log.debug("Building {}...", scraperName);

        this.coordinator = coordinator;

        log.debug("{} trying to register with {}...", scraperName, coordinator.path().name());

        this.coordinator.tell(new CoordinatorProtocol.RegisterMe(), self());

        log.debug("{} trying to spin up sub-scrappers...", scraperName);

        hrefScraper = context().actorOf(HREFScraper.props(), format("%s-%s", scraperName, "HREFScraper"));

        log.debug("{} trying to spin up {}...", scraperName, hrefScraper.path().name());

        imageLinkScraper = context().actorOf(ImageLinkScraper.props(), format("%s-%s", scraperName, "ImageLinkScraper"));

        log.debug("{} trying to spin up {}...", scraperName, imageLinkScraper.path().name());

        hashedBodyScraper = context().actorOf(HashedBodyScraper.props(), format("%s-%s", scraperName, "HashedBodyScraper"));

        log.debug("{} trying to spin up {}...", scraperName, hashedBodyScraper.path().name());

        wordFinderScraper = context().actorOf(WordFinderScraper.props(), format("%s-%s", scraperName, "WordFinderScraper"));

        log.debug("{} trying to spin up {}...", scraperName, wordFinderScraper.path().name());
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorProtocol.WorkAdded.class, msg -> {
                    final String senderName = sender().path().name();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} requesting work from the Coordinator...", scraperName);

                    coordinator.tell(new CoordinatorProtocol.GiveMeWork(), self());
                })
                .match(ScrapperProtocol.ScrapeThis.class, msg -> {
                    final String senderName = sender().path().name();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} delegating work for sub-scrappers...", scraperName);

                    final ScrapeJob scrapeJob = msg.getScrapeJob();
                    final Document document = Jsoup.parse(new URL(scrapeJob.getUrl()), 5000);

                    delegateToSubScrappers(ScrapperProtocol.ScrapeThis.builder().scrapeJob(scrapeJob).document(document).build());
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, scraperName);
                })
                .build();
    }

    private void delegateToSubScrappers(final ScrapperProtocol.ScrapeThis scrapeMessage) {
        log.debug("{} forwarding the scrape request {} to: {}", scraperName, scrapeMessage, hrefScraper);

        hrefScraper.forward(scrapeMessage, context());

        log.debug("{} forwarding the scrape request {} to: {}", scraperName, scrapeMessage, imageLinkScraper);

        imageLinkScraper.forward(scrapeMessage, context());

        log.debug("{} forwarding the scrape request {} to: {}", scraperName, scrapeMessage, hashedBodyScraper);

        hashedBodyScraper.forward(scrapeMessage, context());

        log.debug("{} forwarding the scrape request {} to: {}", scraperName, scrapeMessage, wordFinderScraper);

        wordFinderScraper.forward(scrapeMessage.withWordToFind("adventure"), context());
    }
}