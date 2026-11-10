package com.example.canalcache.controller;

import com.example.canalcache.dto.UserWriteRequest;
import com.example.canalcache.dto.IdBatchRequest;
import com.example.canalcache.entity.User;
import com.example.canalcache.model.BatchQueryResult;
import com.example.canalcache.service.UserQueryService;
import com.example.canalcache.service.UserWriteService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/users")
public class UserController {

    private final UserQueryService userQueryService;
    private final UserWriteService userWriteService;

    public UserController(UserQueryService userQueryService, UserWriteService userWriteService) {
        this.userQueryService = userQueryService;
        this.userWriteService = userWriteService;
    }

    @GetMapping("/{id}")
    public User getById(@PathVariable Long id) {
        User user = userQueryService.getById(id);
        if (user == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
        return user;
    }

    @PostMapping
    public ResponseEntity<Map<String, Long>> create(@RequestBody UserWriteRequest request) {
        validate(request);
        Long id = userWriteService.create(request);
        return ResponseEntity.created(URI.create("/users/" + id)).body(Map.of("id", id));
    }

    @PostMapping("/batch")
    public BatchQueryResult<User> getByIds(@RequestBody IdBatchRequest request) {
        request.validate();
        return userQueryService.getByIds(request.ids());
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void update(@PathVariable Long id, @RequestBody UserWriteRequest request) {
        validate(request);
        if (!userWriteService.update(id, request)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        if (!userWriteService.delete(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }

    private void validate(UserWriteRequest request) {
        // 长度与 docs/demo-users.sql 中的字段一致，只在 HTTP 输入边界校验。
        if (request.name() == null || request.name().isBlank() || request.name().length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "name must be non-blank and at most 64 characters");
        }
        if (request.email() != null && request.email().length() > 128) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "email must be at most 128 characters");
        }
        if (request.phone() != null && request.phone().length() > 32) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "phone must be at most 32 characters");
        }
    }
}
