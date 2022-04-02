package com.transcribestreaming.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamprocessing.kvs.model.TranscribedSegmentWriter;

import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionResponse;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptResultStream;

/**
 * Implementation of StreamTranscriptionBehavior to define how a stream response is handled.
 */
public class StreamTranscriptionBehaviorImpl implements StreamTranscriptionBehavior {

    private static final Logger logger = LoggerFactory.getLogger(StreamTranscriptionBehaviorImpl.class);
    private final TranscribedSegmentWriter segmentWriter;
    private final String tableName;

    public StreamTranscriptionBehaviorImpl(TranscribedSegmentWriter segmentWriter, String tableName) {
        this.segmentWriter = segmentWriter;
        this.tableName = tableName;
    }

    @Override
    public void onError(Throwable e) {
        logger.error("Error in middle of stream: ", e);
    }

    @Override
    public void onStream(TranscriptResultStream e) {
        // EventResultStream has other fields related to the timestamp of the transcripts in it.
        // Please refer to the javadoc of TranscriptResultStream for more details
        segmentWriter.writeToDynamoDB((TranscriptEvent) e, tableName);
    }

    @Override
    public void onResponse(StartStreamTranscriptionResponse r) {
        logger.info(String.format("%d Received Initial response from Transcribe. Request Id: %s",
                System.currentTimeMillis(), r.requestId()));
    }

    @Override
    public void onComplete() {
        logger.info("Transcribe stream completed");
    }
}

