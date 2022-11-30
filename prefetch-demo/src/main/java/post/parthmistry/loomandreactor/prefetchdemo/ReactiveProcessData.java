package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.ElapsedTimeMonitor;
import post.parthmistry.loomandreactor.prefetchdemo.util.PrefetchDemoUtil;
import reactor.core.publisher.Flux;

class ReactiveProcessData {

    private static ElapsedTimeMonitor elapsedTimeMonitor;

    public static void main(String[] args) throws Exception {

        int prefetchSize = 100;

        var enrichedPersonDataIterable = PrefetchDemoUtil.getR2dbcConnection().flatMapMany(connection -> {
            elapsedTimeMonitor = new ElapsedTimeMonitor();
            return Flux.from(connection.createStatement("select * from persons")
                            .fetchSize(100)
                            .execute()
                    ).flatMap(result -> {
                        return result.map((row, metadata) -> {
                            return new PersonData(row.get("id", Integer.class), row.get("name", String.class));
                        });
                    });
        }).flatMap(ReactivePersonDataService::getEnrichedPersonData, 50, prefetchSize).toIterable();

        for (var enrichedPersonData : enrichedPersonDataIterable) {
            System.out.println(enrichedPersonData.id() + " - " + enrichedPersonData.detail() + " -- " + elapsedTimeMonitor.getElapsedTimeMillis());
        }

        System.out.println("total duration: " + elapsedTimeMonitor.getElapsedTimeMillis());
    }

}
