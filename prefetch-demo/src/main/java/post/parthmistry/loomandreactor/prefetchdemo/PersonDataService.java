package post.parthmistry.loomandreactor.prefetchdemo;

import post.parthmistry.loomandreactor.prefetchdemo.data.EnrichedPersonData;
import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import post.parthmistry.loomandreactor.prefetchdemo.util.SleepUtil;

public class PersonDataService {

    public static EnrichedPersonData getEnrichedPersonData(PersonData personData) {
        SleepUtil.sleepFor(personData.id());
        return new EnrichedPersonData(personData.id(), personData.name(), personData.name() + " detail");
    }

}
