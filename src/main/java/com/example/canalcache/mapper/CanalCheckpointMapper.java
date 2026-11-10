package com.example.canalcache.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.canalcache.entity.CanalCheckpointEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * checkpoint 持久化 Mapper。
 */
@Mapper
public interface CanalCheckpointMapper extends BaseMapper<CanalCheckpointEntity> {
}
