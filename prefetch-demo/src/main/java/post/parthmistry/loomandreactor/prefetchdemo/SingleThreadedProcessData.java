package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.util.ElapsedTimeMonitor;
import post.parthmistry.loomandreactor.prefetchdemo.util.PrefetchDemoUtil;

public class SingleThreadedProcessData {

    public static void main(String[] args) throws Exception {
        try (var connection = PrefetchDemoUtil.getConnection()) {
            var elapsedTimeMonitor = new ElapsedTimeMonitor();

            var statement = connection.createStatement();
            statement.setFetchSize(100);

            var resultSet = statement.executeQuery("select * from persons");

            while (resultSet.next()) {
                var personData = PrefetchDemoUtil.createPersonData(resultSet);
                var enrichedPersonData = PersonDataService.getEnrichedPersonData(personData);
                System.out.println(enrichedPersonData.id() + " - " + enrichedPersonData.detail() + " -- " + elapsedTimeMonitor.getElapsedTimeMillis());
            }

            System.out.println("total duration: " + elapsedTimeMonitor.getElapsedTimeMillis());
        }
    }

}
