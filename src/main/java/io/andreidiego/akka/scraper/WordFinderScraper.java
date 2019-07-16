package io.andreidiego.akka.scraper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import static java.lang.String.format;

class WordFinderScraper extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String scraperName;

    static Props props() {
        return Props.create(WordFinderScraper.class);
    }

    public WordFinderScraper() {
        scraperName = self().path().name();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ScrapperProtocol.ScrapeThis.class, msg -> {
                    final String senderName = sender().path().name();
                    final String wortToFind = msg.getWordToFind();

                    log.debug("Message {} received by {} from {}.", msg, scraperName, senderName);
                    log.debug("{} scraping url {}. Searching for the word '{}' in its body content...", scraperName, msg.getScrapeJob().getUrl(),
                              wortToFind);

                    sentencesContainingTheWord(wortToFind, msg.getDocument().body().text()).forEach(sentence -> {
                        log.debug("New sentence containing the word '{}' found by {}: {}", wortToFind, scraperName, sentence);
                    });

                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, scraperName);
                })
                .build();
    }

    private Iterable<String> sentencesContainingTheWord(final String wortToFind, final String sourceText) {
        final List<String> sentences = new ArrayList<>();
        final Pattern pattern = Pattern.compile(format("[^.]* %s [^.]*\\.", wortToFind));
        final Matcher matcher = pattern.matcher(sourceText);

        while (matcher.find()) {
            sentences.add(matcher.group());
        }

        return sentences;
    }
}