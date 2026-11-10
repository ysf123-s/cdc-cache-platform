package com.example.canalcache.service;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.example.canalcache.dto.DepartmentWriteRequest;
import com.example.canalcache.entity.Department;
import com.example.canalcache.mapper.DepartmentMapper;
import com.example.canalcache.model.BatchQueryResult;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DepartmentService {
    private final DepartmentMapper mapper;
    private final CacheQueryService queries;

    public DepartmentService(DepartmentMapper mapper, CacheQueryService queries) {
        this.mapper = mapper;
        this.queries = queries;
    }

    public Department getById(Long id) {
        return queries.getById("departments", id, Department.class);
    }

    public BatchQueryResult<Department> getByIds(List<Long> ids) {
        return queries.getByIds("departments", ids, Department.class);
    }

    public Long create(DepartmentWriteRequest request) {
        Department department = new Department();
        department.setName(request.name());
        department.setCode(request.code());
        mapper.insert(department);
        return department.getId();
    }

    public boolean update(Long id, DepartmentWriteRequest request) {
        return update(id, request, null);
    }

    public boolean update(Long id, DepartmentWriteRequest request, long[] sqlDurationHolder) {
        UpdateWrapper<Department> update = new UpdateWrapper<>();
        update.eq("id", id).set("name", request.name()).set("code", request.code());
        long sqlStartNs = System.nanoTime();
        int rows = mapper.update(null, update);
        long sqlEndNs = System.nanoTime();
        if (sqlDurationHolder != null && sqlDurationHolder.length > 0) {
            sqlDurationHolder[0] = sqlEndNs - sqlStartNs;
        }
        return rows > 0;
    }

    public boolean delete(Long id) {
        return mapper.deleteById(id) > 0;
    }
}
