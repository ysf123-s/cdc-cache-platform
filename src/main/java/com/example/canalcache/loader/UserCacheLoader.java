package com.example.canalcache.loader;

import com.example.canalcache.entity.User;
import com.example.canalcache.mapper.UserMapper;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class UserCacheLoader implements CacheDataLoader<User> {
    private final UserMapper mapper;
    public UserCacheLoader(UserMapper mapper) { this.mapper = mapper; }
    @Override public String table() { return "users"; }
    @Override public Class<User> entityType() { return User.class; }
    @Override public User loadById(Long id) { return mapper.selectById(id); }
    @Override public List<User> loadByIds(List<Long> ids) { return mapper.selectBatchIds(ids); }
    @Override public Long idOf(User entity) { return entity.getId(); }
}
