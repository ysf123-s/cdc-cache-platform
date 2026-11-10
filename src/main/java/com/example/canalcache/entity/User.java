package com.example.canalcache.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.example.canalcache.annotation.AutoCache;
import com.example.canalcache.annotation.CacheStrategy;
import lombok.Data;

import java.util.Date;

/**
 * 用户实体（示例）
 */
@Data
@TableName("users")
@AutoCache(
    table = "users",
    keyTemplate = "user:#id",
    strategy = CacheStrategy.DELETE_MODE,
    expireSeconds = 1800,
    enableVersionControl = false
)
public class User {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private String name;

    private String email;

    private String phone;

    @TableField("created_at")
    private Date createdAt;
}
