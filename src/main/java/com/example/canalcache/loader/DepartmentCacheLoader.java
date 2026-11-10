package com.example.canalcache.loader;

import com.example.canalcache.entity.Department;
import com.example.canalcache.mapper.DepartmentMapper;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class DepartmentCacheLoader implements CacheDataLoader<Department> {
    private final DepartmentMapper mapper;
    public DepartmentCacheLoader(DepartmentMapper mapper) { this.mapper = mapper; }
    @Override public String table() { return "departments"; }
    @Override public Class<Department> entityType() { return Department.class; }
    @Override public Department loadById(Long id) { return mapper.selectById(id); }
    @Override public List<Department> loadByIds(List<Long> ids) { return mapper.selectBatchIds(ids); }
    @Override public Long idOf(Department entity) { return entity.getId(); }
}
