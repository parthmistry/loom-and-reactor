package post.parthmistry.loomandreactor.prefetchdemo.util;

public class SleepUtil {

    private static final int[] predictableRandomNumbers = {
            179, 124, 171, 100, 177,
            115, 169, 118, 172, 102,
            124, 113, 105, 108, 195,
            114, 152, 152, 193, 161,
            177, 158, 112, 128, 153,
            145, 187, 129, 188, 185
    };

    public static int getSleepDuration(int i) {
        return predictableRandomNumbers[i % 30];
    }

    public static void sleepFor(int id) {
        try {
            Thread.sleep(getSleepDuration(id));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
