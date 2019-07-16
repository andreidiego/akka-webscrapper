package com.yardi.akkacourse.coordinator;

import com.yardi.akkacourse.ScrapeJob;
import com.yardi.akkacourse.scraper.Scraper;
import com.yardi.akkacourse.scraper.ScrapperProtocol;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SnapshotOffer;

public class Coordinator extends AbstractPersistentActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    //TODO Externalize these
    private static final int NUMBER_OF_WORKERS = 5;
    private static final int MAX_JOB_RETRIES = 5;
    private static final int MAX_DEPTH = 1;
    private static final String INITIAL_URL = "http://books.toscrape.com";

    private final String coordinatorName;

    // Actor State
    private final List<ActorRef> workers;
    private final Queue<ScrapeJob> pendingWork;

    public static Props props() {
        return Props.create(Coordinator.class);
    }

    private Coordinator() {
        coordinatorName = self().path().name();
        workers = new ArrayList<>();
        pendingWork = new LinkedList<>();

        log.debug("Building {}. Initial config:", coordinatorName);
        log.debug(" NUMBER_OF_WORKERS: {}", NUMBER_OF_WORKERS);
        log.debug(" MAX_JOB_RETRIES: {}", MAX_JOB_RETRIES);
        log.debug(" MAX_DEPTH: {}", MAX_DEPTH);
        log.debug(" INITIAL_URL: {}", INITIAL_URL);

        for (int i = 1; i <= NUMBER_OF_WORKERS; i++) {
            final ActorRef actorRef = context().actorOf(Scraper.props(self()), "Scrapper-" + i);

            log.debug("{} spinning up new Scrapper: {}...", coordinatorName, actorRef.path().name());
        }

        log.debug("{} sending the initial URL ({}) to itself", coordinatorName, INITIAL_URL);

        self().tell(new ScrapperProtocol.NewUrlFound(INITIAL_URL, new ScrapeJob(INITIAL_URL, 0, -1)), self());
    }

    @Override
    public String persistenceId() {
        return coordinatorName;
    }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder()
                .match(CoordinatorProtocol.WorkerRegistered.class, workerRegistered -> {
                    workers.add(workerRegistered.getWorker());
                })
                .match(CoordinatorProtocol.WorkAdded.class, workAdded -> {
                    pendingWork.add(workAdded.getWork());
                })
                .match(CoordinatorProtocol.WorkDistributed.class, workDistributed -> {
                    //TODO This looks odd since we are not using the work which comes with the WorkDistributed message
                    // but, since it is a journal that is being replayed, the end result should be the expected.
                    pendingWork.poll();
                })
                .match(SnapshotOffer.class, snapshotOffer -> {
                    log.debug("Snapshot offer {} received by {}", snapshotOffer, coordinatorName);
                })
                .match(RecoveryCompleted.class, recoveryCompleted -> {
                    // perform init after recovery, before any other messages
                    log.debug("Recovery of {} is complete.", coordinatorName);
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, coordinatorName);
                })
                .build();
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorProtocol.RegisterMe.class, msg -> {
                    final ActorRef worker = sender();
                    final String senderName = worker.path().name();

                    log.debug("Message {} received by {} from {}.", msg, coordinatorName, senderName);
                    log.debug("{} registering worker {}...", coordinatorName, senderName);

                    persist(new CoordinatorProtocol.WorkerRegistered(worker), (CoordinatorProtocol.WorkerRegistered workerRegistered) -> {
                        final ActorRef persistedWorker = workerRegistered.getWorker();
                        getContext().getSystem().eventStream().publish(workerRegistered);

                        registerWorker(persistedWorker);

                        //if (lastSequenceNr() % snapShotInterval == 0 && lastSequenceNr() != 0)
                        //    // IMPORTANT: create a copy of snapshot because ExampleState is mutable
                        //    saveSnapshot(state.copy());

                        log.debug("{} checking if there's work available to send {}...", coordinatorName, senderName);

                        ifWorkAvailableDelegateTo(persistedWorker);
                    });

                })
                .match(ScrapperProtocol.NewUrlFound.class, msg -> {
                    final String senderName = sender().path().name();

                    log.debug("Message {} received by {} from {}.", msg, coordinatorName, senderName);
                    log.debug("{} checking the depth of the message ({}) from {} to make sure it can still process it...", coordinatorName, msg,
                              senderName);

                    // TODO Should we send the work back to sender() instead of adding work to the pool and notifying all workers???
                    ifDepthAllowedAddWorkToThePoolAndNotifyWorkers(msg.getNewURL(), msg.getOriginalScrapeJob());
                })
                .match(CoordinatorProtocol.GiveMeWork.class, msg -> {
                    final String senderName = sender().path().name();

                    log.debug("Message {} received by {} from {}.", msg, coordinatorName, senderName);
                    log.debug("Work requested by {}. {} checking if there's work available to send...", senderName, coordinatorName);

                    ifWorkAvailableDelegateTo(sender());
                })
                //TODO Handle exponential back-off on retries of failed scrapes
                .match(ScrapperProtocol.ScrapeFailed.class, msg -> {
                    final String senderName = sender().path().name();
                    final ScrapeJob scrapeJob = msg.getScrapeJob();

                    log.debug("Message {} received by {} from {}.", msg, coordinatorName, senderName);
                    log.debug("{} failed scraping {}. {} checking if it can still retry scraping it later...", senderName,
                              scrapeJob.getUrl(), coordinatorName);

                    if (jobRetriesNotExhausted(scrapeJob)) {
                        log.debug("{} incrementing {} failures and scheduling it for retrying later...", scrapeJob);

                        incrementJobFailuresAndScheduleForRetry(scrapeJob);
                    }
                })
                .matchAny(msg -> {
                    final String senderName = sender().path().name();

                    log.info("Message {} from {} discarded by {}.", msg, senderName, coordinatorName);
                })
                .build();
    }

    private void registerWorker(final ActorRef worker) {
        workers.add(worker);

        log.debug("{} added a new worker to the pool of workers: {}", coordinatorName, worker.path().name());
    }

    private void ifWorkAvailableDelegateTo(final ActorRef worker) {
        if (thereIsPendingWork()) {
            sendWorkTo(worker);
        }
    }

    private boolean thereIsPendingWork() {
        final boolean workAvailable = !pendingWork.isEmpty();

        log.debug("Work available in {}? {}", coordinatorName, workAvailable ? "YES" : "NO");

        return workAvailable;
    }

    private void sendWorkTo(final ActorRef worker) {

        persist(new CoordinatorProtocol.WorkDistributed(pendingWork.peek()), (CoordinatorProtocol.WorkDistributed workDistributed) -> {
            final ScrapeJob nextAvailableWork = nextAvailableWork();
            getContext().getSystem().eventStream().publish(workDistributed);

            log.debug("{} sending {} to worker {}...", coordinatorName, nextAvailableWork, worker.path().name());

            worker.tell(ScrapperProtocol.ScrapeThis.builder().scrapeJob(nextAvailableWork).build(), self());
        });
    }

    private ScrapeJob nextAvailableWork() {
        return pendingWork.poll();
    }

    private void ifDepthAllowedAddWorkToThePoolAndNotifyWorkers(final String url, final ScrapeJob originalScrapeJob) {
        final int newJobDepth = originalScrapeJob.getDepth() + 1;

        log.debug("New job depth is {}. The max depth allowed is {}. {} {} add the new work to the pool.",
                  newJobDepth, MAX_DEPTH, coordinatorName, newJobDepth <= MAX_DEPTH ? "can" : "cannot");

        if (newJobDepth <= MAX_DEPTH) {
            final ScrapeJob newScrapeJob = new ScrapeJob(url, 0, newJobDepth);

            log.debug("{} adding {} to the work pool...", coordinatorName, newScrapeJob);

            pendingWork.add(newScrapeJob);
            notifyWorkers(newScrapeJob);
        }
    }

    private void notifyWorkers(final ScrapeJob newWork) {
        workers.forEach(worker -> {
            log.debug("{} notifying worker {} about the new work available...", coordinatorName, worker.path().name());

            worker.tell(new CoordinatorProtocol.WorkAdded(newWork), self());
        });
    }

    private boolean jobRetriesNotExhausted(final ScrapeJob scrapeJob) {
        log.debug("Job {} already failed {} time(s). {} more retries are allowed.", scrapeJob, scrapeJob.getFailureCount(),
                  MAX_JOB_RETRIES - scrapeJob.getFailureCount());

        return scrapeJob.getFailureCount() < MAX_JOB_RETRIES;
    }

    private void incrementJobFailuresAndScheduleForRetry(final ScrapeJob scrapeJob) {
        pendingWork.add(new ScrapeJob(scrapeJob.getUrl(), scrapeJob.getFailureCount() + 1, scrapeJob.getDepth()));

        log.debug("{} re-added {} to the work pool.", coordinatorName, scrapeJob);
    }
}