package com.example.canalcache.service;

import com.baomidou.mybatisplus.annotation.TableId;
import com.example.canalcache.annotation.AutoCache;
import com.example.canalcache.model.AutoCacheConfig;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @AutoCache 注解处理器
 */
@Slf4j
@Component
public class CacheAnnotationProcessor {

    @Value("${canal.cache.base-package:com.example.canalcache}")
    private String basePackage;

    /**
     * 表名 → 缓存配置的映射
     */
    private final Map<String, AutoCacheConfig> tableConfigMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        scanAndRegister();
    }

    /**
     * 扫描所有带 @AutoCache 注解的类并注册
     */
    private void scanAndRegister() {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(AutoCache.class));

        for (var beanDefinition : scanner.findCandidateComponents(basePackage)) {
            try {
                Class<?> beanType = Class.forName(beanDefinition.getBeanClassName());
                AutoCache annotation = beanType.getAnnotation(AutoCache.class);
                if (annotation != null) {
                    registerConfig(beanType, annotation);
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Cannot load cache entity " + beanDefinition.getBeanClassName(), e);
            }
        }

        log.info("CacheAnnotationProcessor initialized with {} configs from package {}",
                tableConfigMap.size(), basePackage);
    }

    private void registerConfig(Class<?> clazz, AutoCache annotation) {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setTable(annotation.table());
        config.setEntityClass(clazz);
        config.setKeyTemplate(annotation.keyTemplate());
        config.setStrategy(annotation.strategy());
        config.setExpireSeconds(annotation.expireSeconds());
        config.setMaxSizeBytes(annotation.maxSizeBytes());
        config.setEnableVersionControl(annotation.enableVersionControl());

        extractIdFieldInfo(clazz, config);
        validateConfig(config);
        if (tableConfigMap.containsKey(config.getTable())) {
            throw new IllegalStateException("Duplicate cache table: " + config.getTable());
        }
        String prefix = config.keyPrefix();
        for (AutoCacheConfig existing : tableConfigMap.values()) {
            if (prefix.startsWith(existing.keyPrefix()) || existing.keyPrefix().startsWith(prefix)) {
                throw new IllegalStateException("Overlapping cache prefixes: " + config.getKeyTemplate()
                        + " and " + existing.getKeyTemplate());
            }
        }
        tableConfigMap.put(config.getTable(), config);
        log.info("Registered @AutoCache for table: {}, strategy: {}",
                annotation.table(), annotation.strategy());

    }

    private void validateConfig(AutoCacheConfig config) {
        String context = "Invalid @AutoCache on " + config.getEntityClass().getName() + ": ";
        if (!config.getTable().matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalStateException(context + "table must be a single table name");
        }
        if (!config.getKeyTemplate().matches("[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*:#id")) {
            throw new IllegalStateException(context + "keyTemplate must use a fixed namespace followed by :#id");
        }
        if (config.getExpireSeconds() <= 0 || config.getMaxSizeBytes() <= 0) {
            throw new IllegalStateException(context + "expireSeconds and maxSizeBytes must be positive");
        }
        if (config.getIdFieldType() != Long.class && config.getIdFieldType() != long.class) {
            throw new IllegalStateException(context + "only Long/long primary keys are supported");
        }
    }

    private void extractIdFieldInfo(Class<?> clazz, AutoCacheConfig config) {
        Field annotatedId = null;
        // 优先查找 MyBatis-Plus @TableId 字段。binlog 数据使用数据库列名，所以优先使用 value。
        for (Field field : clazz.getDeclaredFields()) {
            TableId tableId = field.getAnnotation(TableId.class);
            if (tableId != null) {
                if (annotatedId != null) {
                    throw new IllegalStateException("Multiple @TableId fields in " + clazz.getName());
                }
                annotatedId = field;
            }
        }
        if (annotatedId != null) {
            String idColumn = annotatedId.getAnnotation(TableId.class).value();
            config.setIdField(idColumn.isBlank() ? annotatedId.getName() : idColumn);
            config.setIdFieldType(annotatedId.getType());
            return;
        }

        // 回退：查找名为 id 的字段
        try {
            Field idField = clazz.getDeclaredField("id");
            config.setIdField("id");
            config.setIdFieldType(idField.getType());
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("No @TableId or id field in " + clazz.getName(), e);
        }
    }

    /**
     * 根据表名获取缓存配置
     */
    public AutoCacheConfig getConfig(String tableName) {
        return tableConfigMap.get(tableName);
    }

    /**
     * 判断某表是否需要缓存同步
     */
    public boolean hasConfig(String tableName) {
        return tableConfigMap.containsKey(tableName);
    }

    /**
     * 获取所有已注册的表名
     */
    public Set<String> getAllTables() {
        return Collections.unmodifiableSet(tableConfigMap.keySet());
    }
}
