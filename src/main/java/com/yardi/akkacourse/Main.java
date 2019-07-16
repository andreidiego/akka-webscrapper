package com.yardi.akkacourse;

import com.yardi.akkacourse.coordinator.Coordinator;

import akka.actor.ActorSystem;

// TODO Externalize config;
// TODO Introduce persistence;
// TODO Find the best way to terminate the actor system;
// TODO Think about supervision strategies:
// TODO Scaling:
// TODO 	Provide a way of adding workers dynamically (without restarting the application);
// TODO 	    Does the Coordinator need to DeathWatch on Workers???
// TODO 	    What should a scraper do should any of its sub-scrappers fail/die???
// TODO 	Clusterize it.
// TODO Rethink work distribution (we may have more messages back and forth than we really need);
// TODO Introduce exponential back-off on retries;
// TODO Think about having a PageLoader actor:
// TODO 	For not requesting the same page content more than once;
// TODO 	What if two scrapers want to load the same url at the same time?
class Main {

    public static void main(final String[] args) {
        final ActorSystem actorSystem = ActorSystem.create("WebScraper");
        actorSystem.actorOf(Coordinator.props(), "Coordinator");
    }
}