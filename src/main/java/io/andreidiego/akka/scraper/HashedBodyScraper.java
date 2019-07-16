package io.andreidiego.akka.scraper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import static java.nio.charset.StandardCharsets.UTF_8;

class HashedBodyScraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;

    static Props props() {
        return Props.create(HashedBodyScraper.class);
    }

    public HashedBodyScraper() {
        scraperName = self().path().name();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ScrapperProtocol.ScrapeThis.class, msg -> {
                    final String senderName = sender().path().name();
                    final String baseURL = msg.getScrapeJob().getUrl();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} scraping url {}. Creating a hash of its body content...", scraperName, baseURL);

                    final String hashedBodyContent = hash(msg.getDocument().body().text());

                    log.debug("Content of {}'s body content's been hashed by {}. Here's what it got: {}", baseURL, scraperName, hashedBodyContent);
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, scraperName);
                })
                .build();
    }

    private String hash(final String text) {
        final MessageDigest md5;

        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return DatatypeConverter.printHexBinary(md5.digest(text.getBytes(UTF_8))).toUpperCase();
    }
}