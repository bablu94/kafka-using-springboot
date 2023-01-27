package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    ResponseEntity<?> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendEventListenerApproach2(libraryEvent);
        return  ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        if(libraryEvent.getLibraryEventId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("please pass the libraryeventId");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendEventListenerApproach2(libraryEvent);
        return  ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
