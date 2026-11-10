package com.example.canalcache.controller;

import com.example.canalcache.dto.DepartmentWriteRequest;
import com.example.canalcache.dto.IdBatchRequest;
import com.example.canalcache.entity.Department;
import com.example.canalcache.model.BatchQueryResult;
import com.example.canalcache.service.DepartmentService;
import com.example.canalcache.diagnostics.TraceCollector;
import org.springframework.beans.factory.annotation.Autowired;
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
@RequestMapping("/departments")
public class DepartmentController {
    private final DepartmentService service;
    private final TraceCollector traceCollector;

    public DepartmentController(DepartmentService service) {
        this(service, new TraceCollector());
    }

    @Autowired
    public DepartmentController(DepartmentService service, TraceCollector traceCollector) {
        this.service = service;
        this.traceCollector = traceCollector;
    }

    @GetMapping("/{id}")
    public Department getById(@PathVariable Long id) {
        Department department = service.getById(id);
        if (department == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }
        return department;
    }

    @PostMapping("/batch")
    public BatchQueryResult<Department> getByIds(@RequestBody IdBatchRequest request) {
        request.validate();
        return service.getByIds(request.ids());
    }

    @PostMapping
    public ResponseEntity<Map<String, Long>> create(@RequestBody DepartmentWriteRequest request) {
        validate(request);
        Long id = service.create(request);
        return ResponseEntity.created(URI.create("/departments/" + id)).body(Map.of("id", id));
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void update(@PathVariable Long id, @RequestBody DepartmentWriteRequest request) {
        validate(request);
        long startNs = System.nanoTime();
        long startEpochMs = System.currentTimeMillis();

        long[] sqlDuration = new long[1];
        boolean updated = service.update(id, request, sqlDuration);

        long endNs = System.nanoTime();
        long endEpochMs = System.currentTimeMillis();

        if (!updated) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }

        traceCollector.recordWrite(request.code(), id, startEpochMs, endEpochMs,
                endNs - startNs, sqlDuration[0]);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        if (!service.delete(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }
    }

    private void validate(DepartmentWriteRequest request) {
        if (request.name() == null || request.name().isBlank() || request.name().length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "name must be non-blank and at most 64 characters");
        }
        if (request.code() != null && request.code().length() > 32) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "code must be at most 32 characters");
        }
    }
}
