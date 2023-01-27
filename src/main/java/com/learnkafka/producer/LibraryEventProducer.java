package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    String topic = "library-events";

    public void sendEventListener(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent.getBook());
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
               handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
               handleSuccess(key, value, result);
            }
        });
    }

    public void sendEventListenerApproach2(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent.getBook());

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> headerList = List.of(new RecordHeader("event source","scanner".getBytes()));
        return new ProducerRecord<Integer, String>(topic, null, key, value , headerList);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent.getBook());
        SendResult<Integer, String> sendResult = null;
        try {
          sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("error sending the message: {}",e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("error sending the message: {}",e.getMessage());
        }

        return sendResult;
    }

    private void handleFailure(Integer key, String value, Throwable ex){
        log.error("error sending the message: {}",ex.getMessage());
        try {
            throw ex;
        } catch (Throwable e) {
            log.error("error on failure:{}", e.getMessage());
        }

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("message sent successfully for the key: {} and value is {} and partition is {}"
        ,key, value, result.getRecordMetadata().partition());
    }
}
