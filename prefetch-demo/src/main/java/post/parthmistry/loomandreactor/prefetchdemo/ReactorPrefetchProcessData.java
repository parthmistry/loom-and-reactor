package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.ElapsedTimeMonitor;
import post.parthmistry.loomandreactor.prefetchdemo.util.PrefetchDemoUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class PersonDataRetrieverForReactor implements Runnable {

    private final Connection connection;

    private final FluxSink<PersonData> fluxSink;

    private long currentDemand;

    private final Lock lock;

    private final Condition waitForDemand;

    public PersonDataRetrieverForReactor(Connection connection, FluxSink<PersonData> fluxSink) {
        this.connection = connection;
        this.fluxSink = fluxSink;
        this.currentDemand = 0;
        this.lock = new ReentrantLock();
        this.waitForDemand = lock.newCondition();
        this.fluxSink.onRequest(this::updateDemand);
    }

    private void updateDemand(long newRequestCount) {
        lock.lock();
        try {
            currentDemand += newRequestCount;
            waitForDemand.signal();
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        try (var statement = connection.createStatement()) {
            statement.setFetchSize(100);

            var resultSet = statement.executeQuery("select * from persons");

            while (resultSet.next()) {
                var personData = PrefetchDemoUtil.createPersonData(resultSet);
                lock.lock();
                try {
                    while (currentDemand == 0) {
                        waitForDemand.await();
                    }
                    fluxSink.next(personData);
                    currentDemand--;
                } finally {
                    lock.unlock();
                }
            }

            fluxSink.complete();

        } catch (Exception e) {
            System.out.println("Unexpected error occurred: " + e.getMessage());
            fluxSink.error(e);
        }
    }

}

public class ReactorPrefetchProcessData {

    public static void main(String[] args) throws Exception {

        int prefetchSize = 100;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var connection = PrefetchDemoUtil.getConnection()) {

            var elapsedTimeMonitor = new ElapsedTimeMonitor();

            var personDataFlux = Flux.create((FluxSink<PersonData> fluxSink) -> {
                var personDataRetrieverForReactor = new PersonDataRetrieverForReactor(connection, fluxSink);
                executor.submit(personDataRetrieverForReactor);
            });

            var enrichedPersonDataIterable = personDataFlux.limitRate(prefetchSize, Double.valueOf(prefetchSize * 0.20).intValue())
                    .flatMap(ReactivePersonDataService::getEnrichedPersonData, 50, prefetchSize)
                    .toIterable();

            for (var enrichedPersonData : enrichedPersonDataIterable) {
                System.out.println(enrichedPersonData.id() + " - " + enrichedPersonData.detail() + " -- " + elapsedTimeMonitor.getElapsedTimeMillis());
            }

            System.out.println("total duration: " + elapsedTimeMonitor.getElapsedTimeMillis());
        }
    }

}
