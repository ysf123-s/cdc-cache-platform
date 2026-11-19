package com.example.canalcache.loader;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
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

    @Override
    public List<Long> loadIdsByIndex(String column, String value) {
        if (!"email".equals(column)) {
            return CacheDataLoader.super.loadIdsByIndex(column, value);
        }
        return mapper.selectList(new QueryWrapper<User>().select("id", "email").eq("email", value)).stream()
                .filter(user -> value.equals(user.getEmail())).map(User::getId).toList();
    }
}
