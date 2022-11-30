package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.ElapsedTimeMonitor;
import post.parthmistry.loomandreactor.prefetchdemo.util.PrefetchDemoUtil;
import post.parthmistry.loomandreactor.prefetchdemo.util.SemaphoreWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class BasicMultiThreadedProcessData {

    public static void main(String[] args) throws Exception {
        var semaphore = new SemaphoreWrapper(50);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var connection = PrefetchDemoUtil.getConnection()) {

            var elapsedTimeMonitor = new ElapsedTimeMonitor();

            var statement = connection.createStatement();
            statement.setFetchSize(100);

            var resultSet = statement.executeQuery("select * from persons");

            List<PersonData> personDataList = getNextBatch(resultSet);

            while (!personDataList.isEmpty()) {
                var callables = personDataList.stream().map(personData ->
                    (Callable<Void>) () -> {
                        semaphore.acquire();
                        try {
                            var enrichedPersonData = PersonDataService.getEnrichedPersonData(personData);
                            System.out.println(enrichedPersonData.id() + " - " + enrichedPersonData.detail() + " -- " + elapsedTimeMonitor.getElapsedTimeMillis());
                        } finally {
                            semaphore.release();
                        }
                        return null;
                    }
                ).toList();
                executor.invokeAll(callables);
                personDataList = getNextBatch(resultSet);
            }

            System.out.println("total duration: " + elapsedTimeMonitor.getElapsedTimeMillis());
        }
    }

    public static List<PersonData> getNextBatch(ResultSet resultSet) throws SQLException {
        List<PersonData> personDataList = new ArrayList<>();
        for (int i = 0; i < 100 && resultSet.next(); i++) {
            personDataList.add(PrefetchDemoUtil.createPersonData(resultSet));
        }
        return personDataList;
    }

}
