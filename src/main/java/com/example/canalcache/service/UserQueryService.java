package com.example.canalcache.service;

import com.example.canalcache.entity.User;
import com.example.canalcache.model.BatchQueryResult;
import org.springframework.stereotype.Service;

import java.util.List;

/** 用户表接入共享缓存查询流程。 */
@Service
public class UserQueryService {
    private final CacheQueryService queries;

    public UserQueryService(CacheQueryService queries) {
        this.queries = queries;
    }

    public User getById(Long id) {
        return queries.getById("users", id, User.class);
    }

    public BatchQueryResult<User> getByIds(List<Long> ids) {
        return queries.getByIds("users", ids, User.class);
    }

    /** 按邮箱精确查询（区分大小写）。索引列表与实体缓存之间有异步窗口，返回前再核对邮箱。 */
    public List<User> getByEmail(String email) {
        return queries.getByIndex("users", "email", email, User.class).stream()
                .filter(user -> email.equals(user.getEmail())).toList();
    }
}
