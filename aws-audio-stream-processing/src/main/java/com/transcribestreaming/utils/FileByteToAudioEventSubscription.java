package com.transcribestreaming.utils;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.transcribestreaming.model.AudioEvent;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Subscription converts audio bytes read from a File InputStream into AudioEvents
 */
public class FileByteToAudioEventSubscription implements Subscription {

    private static final int CHUNK_SIZE_IN_BYTES = 4;
    private ExecutorService executor = Executors.newFixedThreadPool(1);
    private AtomicLong demand = new AtomicLong(0);
    private final Subscriber<? super AudioStream> subscriber;
    private final InputStream inputStream;

    public FileByteToAudioEventSubscription(Subscriber<? super AudioStream> s, InputStream inputStream) {
        this.subscriber = s;
        this.inputStream = inputStream;
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            subscriber.onError(new IllegalArgumentException("Demand must be positive"));
        }

        demand.getAndAdd(n);
        //We need to invoke this in a separate thread because the call to subscriber.onNext(...) is recursive
        executor.submit(() -> {
            try {
                while (demand.get() > 0) {
                    ByteBuffer audioBuffer = getNextByteBuffer();

                    if (audioBuffer.remaining() > 0) {

                        AudioEvent audioEvent = audioEventFromBuffer(audioBuffer);
                        subscriber.onNext(audioEvent);

                    } else {
                        subscriber.onComplete();
                        break;
                    }
                    demand.getAndDecrement();
                }
            } catch (Exception e) {
                subscriber.onError(e);
            }
        });
    }

    @Override
    public void cancel() {
        executor.shutdown();
    }

    private AudioEvent audioEventFromBuffer(ByteBuffer bb) {
        return AudioEvent.builder()
                .audioChunk(SdkBytes.fromByteBuffer(bb))
                .build();
    }

    private ByteBuffer getNextByteBuffer() throws IOException {

        byte[] audioBytes = new byte[CHUNK_SIZE_IN_BYTES];

        int len = inputStream.read(audioBytes);
        if (len <=0) {
            return ByteBuffer.allocate(0);
        } else {
            return ByteBuffer.wrap(audioBytes, 0, len);
        }
    }
}
