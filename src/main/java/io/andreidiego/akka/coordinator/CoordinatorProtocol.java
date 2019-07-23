package io.andreidiego.akka.coordinator;

import io.andreidiego.akka.ScrapeJob;

import java.io.Serializable;

import akka.actor.ActorRef;
import lombok.Value;

public class CoordinatorProtocol {

    private interface Command {
    }

    @Value
    public static class RegisterMe implements Command {
    }

    @Value
    public static class GiveMeWork implements Command {
    }

    @Value
    static class Shutdown implements Command {
    }

    interface Event extends Serializable {
    }

    @Value
    static class WorkerRegistered implements Event {
        private final ActorRef worker;
    }

    @Value
    public static class WorkAdded implements Event {
        private final ScrapeJob work;
    }

    @Value
    static class WorkDistributed implements Event {
        private final ScrapeJob work;
    }
}