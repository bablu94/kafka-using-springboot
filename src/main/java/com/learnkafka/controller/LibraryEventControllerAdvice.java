package com.learnkafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.List;
import java.util.stream.Collectors;

@ControllerAdvice
@Slf4j
public class LibraryEventControllerAdvice {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> handleRequestBody(MethodArgumentNotValidException e){

       List<FieldError> errorList = e.getBindingResult().getFieldErrors();
       String errorMessage = errorList.stream()
               .map(fieldError -> fieldError.getField() +"-"+fieldError.getDefaultMessage())
               .sorted()
               .collect(Collectors.joining("-"));
       log.error("error message:{}", errorMessage);
       return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
    }
}
