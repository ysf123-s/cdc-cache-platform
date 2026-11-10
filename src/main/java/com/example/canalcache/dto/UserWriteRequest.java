package com.example.canalcache.dto;

/**
 * 写接口只接收用户资料，ID 和创建时间由数据库维护。
 * PUT 为资料整体替换，email/phone 省略或为 null 时清空对应字段。
 */
public record UserWriteRequest(String name, String email, String phone) {
}
