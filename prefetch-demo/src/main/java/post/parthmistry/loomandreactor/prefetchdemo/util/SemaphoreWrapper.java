package post.parthmistry.loomandreactor.prefetchdemo.util;

import java.util.concurrent.Semaphore;

public class SemaphoreWrapper {

    private final Semaphore semaphore;

    public SemaphoreWrapper(int permits) {
        this.semaphore = new Semaphore(permits);
    }

    public void acquire() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void release() {
        semaphore.release();
    }

}
