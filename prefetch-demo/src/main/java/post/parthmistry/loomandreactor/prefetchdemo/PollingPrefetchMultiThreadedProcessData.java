package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.ElapsedTimeMonitor;
import post.parthmistry.loomandreactor.prefetchdemo.util.PrefetchDemoUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class PersonDataRetriever implements Runnable {

    private final Connection connection;

    private final int prefetchSize;

    private final List<ProcessPersonDataTask> waitingTasks;

    private final List<PersonData> prefetchedList;

    private boolean fetchFinished;

    private final Lock lock;

    private final Condition waitCondition;

    public PersonDataRetriever(Connection connection, int prefetchSize) {
        this.connection = connection;
        this.prefetchSize = prefetchSize;
        this.waitingTasks = new LinkedList<>();
        this.prefetchedList = new LinkedList<>();
        this.fetchFinished = false;
        this.lock = new ReentrantLock();
        this.waitCondition = lock.newCondition();
    }

    public boolean pollPersonData(ProcessPersonDataTask task) {
        lock.lock();
        try {
            if (!prefetchedList.isEmpty()) {
                task.sendPersonData(Optional.of(prefetchedList.remove(0)));
                waitCondition.signal();
                return false;
            } else if (!fetchFinished) {
                waitingTasks.add(task);
                waitCondition.signal();
                return true;
            } else {
                task.sendPersonData(Optional.empty());
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        try (var statement = connection.createStatement()) {
            statement.setFetchSize(100);

            var resultSet = statement.executeQuery("select * from persons");

            while (resultSet.next()) {
                var personData = PrefetchDemoUtil.createPersonData(resultSet);
                lock.lock();
                try {
                    if (!waitingTasks.isEmpty()) {
                        waitingTasks.remove(0).sendPersonData(Optional.of(personData));
                    } else if (prefetchedList.size() < prefetchSize) {
                        prefetchedList.add(personData);
                    } else {
                        waitCondition.await();
                        prefetchedList.add(personData);
                    }
                } finally {
                    lock.unlock();
                }
            }

            lock.lock();
            try {
                fetchFinished = true;
                waitingTasks.forEach(waitingTask -> waitingTask.sendPersonData(Optional.empty()));
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class ProcessPersonDataTask implements Runnable {

    private final PersonDataRetriever personDataRetriever;

    private final ElapsedTimeMonitor elapsedTimeMonitor;

    private Optional<PersonData> personDataOptional;

    private final Lock lock;

    private final Condition waitCondition;

    ProcessPersonDataTask(PersonDataRetriever personDataRetriever, ElapsedTimeMonitor elapsedTimeMonitor) {
        this.personDataRetriever = personDataRetriever;
        this.elapsedTimeMonitor = elapsedTimeMonitor;
        this.personDataOptional = Optional.empty();
        this.lock = new ReentrantLock();
        this.waitCondition = lock.newCondition();
    }

    public void sendPersonData(Optional<PersonData> personDataOptional) {
        lock.lock();
        try {
            this.personDataOptional = personDataOptional;
            waitCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public Optional<PersonData> pollNextPersonData() {
        lock.lock();
        try {
            var needWait = personDataRetriever.pollPersonData(this);
            if (needWait) {
                try {
                    waitCondition.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return personDataOptional;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        while (true) {
            var personDataOptional = pollNextPersonData();
            if (personDataOptional.isPresent()) {
                var personData = personDataOptional.get();
                var enrichedPersonData = PersonDataService.getEnrichedPersonData(personData);
                System.out.println(enrichedPersonData.id() + " - " + enrichedPersonData.detail() + " -- " + elapsedTimeMonitor.getElapsedTimeMillis());
            } else {
                break;
            }
        }
    }

}

public class PollingPrefetchMultiThreadedProcessData {

    public static void main(String[] args) throws Exception {

        int prefetchSize = 100;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var connection = PrefetchDemoUtil.getConnection()) {

            var elapsedTimeMonitor = new ElapsedTimeMonitor();

            var personDataRetriever = new PersonDataRetriever(connection, prefetchSize);
            executor.submit(personDataRetriever);

            var futures = new ArrayList<Future<?>>();

            for (int i = 0; i < 50; i++) {
                var future = executor.submit(new ProcessPersonDataTask(personDataRetriever, elapsedTimeMonitor));
                futures.add(future);
            }

            for (var future : futures) {
                future.get();
            }

            System.out.println("total duration: " + elapsedTimeMonitor.getElapsedTimeMillis());
        }
    }

}
