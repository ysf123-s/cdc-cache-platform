package com.example.canalcache.platform;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.NoSuchElementException;

@RestControllerAdvice(basePackages = "com.example.canalcache.platform")
public class PlatformExceptionHandler {
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetail> invalid(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, error.getMessage()));
    }
    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<ProblemDetail> missing(NoSuchElementException error) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, error.getMessage()));
    }
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ProblemDetail> conflict(IllegalStateException error) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, error.getMessage()));
    }
}
