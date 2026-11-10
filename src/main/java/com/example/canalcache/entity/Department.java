package com.example.canalcache.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.example.canalcache.annotation.AutoCache;
import com.example.canalcache.annotation.CacheStrategy;
import lombok.Data;

/** 用第二张表验证同一查询和同步流程可复用。 */
@Data
@TableName("departments")
@AutoCache(table = "departments", keyTemplate = "department:#id", strategy = CacheStrategy.DELETE_MODE,
        expireSeconds = 600, enableVersionControl = false)
public class Department {
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;
    private String name;
    private String code;
}
