package io.andreidiego.akka.scraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.URL;
import java.util.Set;
import java.util.stream.Stream;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.andreidiego.akka.ScrapeJob;
import io.andreidiego.akka.coordinator.CoordinatorProtocol;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class Scraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;
    private final ActorRef coordinator;
    private final Set<ActorRef> subScrappers;

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

        subScrappers = spinUpSubScrappers(Stream.of(HREFScraper.props(),
                                     ImageLinkScraper.props(),
                                     HashedBodyScraper.props(),
                                     WordFinderScraper.props()));
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

    private Set<ActorRef> spinUpSubScrappers(final Stream<Props> propsStream) {
        return propsStream.map(props -> {
            final String subScrapperName = props.actorClass().getSimpleName();

            log.debug("{} trying to spin up {}-{}...", scraperName, scraperName, subScrapperName);

            return context().actorOf(props, format("%s-%s", scraperName, subScrapperName));
        }).collect(toSet());
    }

    private void delegateToSubScrappers(final ScrapperProtocol.ScrapeThis scrapeMessage) {
        subScrappers.forEach(subScrapper -> {

            log.debug("{} forwarding the scrape request {} to {}...", scraperName, scrapeMessage, subScrapper.path().name());

            subScrapper.tell(scrapeMessage, self());
        });
    }
}