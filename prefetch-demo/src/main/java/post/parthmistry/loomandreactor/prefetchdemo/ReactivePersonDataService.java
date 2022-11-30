package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.EnrichedPersonData;
import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.SleepUtil;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class ReactivePersonDataService {

    public static Mono<EnrichedPersonData> getEnrichedPersonData(PersonData personData) {
        var delayMillis = SleepUtil.getSleepDuration(personData.id());
        return Mono.delay(Duration.ofMillis(delayMillis))
                .map(d -> new EnrichedPersonData(personData.id(), personData.name(), personData.name() + " detail"));
    }

}
