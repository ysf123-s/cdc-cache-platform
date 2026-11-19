package com.example.canalcache.service;

import com.baomidou.mybatisplus.annotation.TableId;
import com.example.canalcache.annotation.AutoCache;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.entity.User;
import com.example.canalcache.entity.Department;
import com.example.canalcache.loader.*;
import com.example.canalcache.mapper.UserMapper;
import com.example.canalcache.mapper.DepartmentMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheRegistrationTest {
    @Test
    void springCollectsBothLoadersAndMatchesScannedEntities() {
        context().withBean(UserCacheLoader.class).withBean(DepartmentCacheLoader.class).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            var registry = ctx.getBean(CacheLoaderRegistry.class);
            assertThat(registry.get("users", User.class)).isInstanceOf(UserCacheLoader.class);
            assertThat(registry.get("departments", Department.class)).isInstanceOf(DepartmentCacheLoader.class);
            assertThatThrownBy(() -> registry.get("users", Department.class)).hasMessageContaining("Wrong entity type");
            assertThatThrownBy(() -> registry.get("unknown", User.class)).hasMessageContaining("Unknown cache group");
        });
    }

    @Test
    void missingLoaderPreventsStartup() {
        context().withBean(UserCacheLoader.class).run(ctx ->
                assertThat(ctx).hasFailed().getFailure().hasRootCauseMessage("Missing cache loader for departments"));
    }

    @Test
    void duplicateLoaderPreventsStartup() {
        context().withBean(UserCacheLoader.class).withBean(DepartmentCacheLoader.class)
                .withBean("duplicateUser", UserCacheLoader.class, () -> new UserCacheLoader(mock(UserMapper.class)))
                .run(ctx -> assertThat(ctx).hasFailed().getFailure()
                        .hasRootCauseMessage("Duplicate cache loader for users"));
    }

    @Test
    void unregisteredLoaderAndWrongTypeAreRejected() {
        var processor = scanned();
        CacheDataLoader<User> loader = mock(CacheDataLoader.class);
        when(loader.table()).thenReturn("unknown"); when(loader.entityType()).thenReturn(User.class);
        assertThatThrownBy(() -> new CacheLoaderRegistry(List.of(loader), processor))
                .hasMessageContaining("not registered");
        when(loader.table()).thenReturn("departments");
        assertThatThrownBy(() -> new CacheLoaderRegistry(List.of(loader), processor))
                .hasMessageContaining("entity type mismatch");
    }

    @ParameterizedTest
    @ValueSource(strings = {"duplicateTable", "overlap", "samePrefix", "zeroTtl", "negativeTtl", "zeroSize",
            "badTemplate", "noPrefix", "middlePlaceholder", "badTable", "missingId", "stringId", "multipleIds",
            "reservedPrefix", "reservedSubPrefix", "badIndexTemplate", "unknownIndexColumn", "indexOverlapsMainPrefix",
            "indexOverlapsOtherGroup"})
    void invalidEntityConfigurationCannotStartApplication(String problem) {
        AutoCache annotation = annotation("books", "book:#id");
        Class<?> entity = LongId.class;
        switch (problem) {
            case "zeroTtl" -> when(annotation.expireSeconds()).thenReturn(0);
            case "negativeTtl" -> when(annotation.expireSeconds()).thenReturn(-1);
            case "zeroSize" -> when(annotation.maxSizeBytes()).thenReturn(0);
            case "badTemplate" -> when(annotation.keyTemplate()).thenReturn("book*:#id");
            case "noPrefix" -> when(annotation.keyTemplate()).thenReturn("#id");
            case "middlePlaceholder" -> when(annotation.keyTemplate()).thenReturn("book:#id:copy");
            case "badTable" -> when(annotation.table()).thenReturn("demo.books");
            case "reservedPrefix" -> when(annotation.keyTemplate()).thenReturn("canal-cache:#id");
            case "reservedSubPrefix" -> when(annotation.keyTemplate()).thenReturn("canal-cache:books:#id");
            case "badIndexTemplate" -> when(annotation.indexKeys()).thenReturn(new String[]{"book-id:id"});
            case "unknownIndexColumn" -> when(annotation.indexKeys()).thenReturn(new String[]{"book-isbn:#isbn"});
            case "indexOverlapsMainPrefix" -> when(annotation.indexKeys()).thenReturn(new String[]{"book:#id"});
            case "missingId" -> entity = MissingId.class;
            case "stringId" -> entity = StringId.class;
            case "multipleIds" -> entity = MultipleIds.class;
        }
        Class<?> selected = entity;
        new ApplicationContextRunner().withBean(CacheAnnotationProcessor.class, () -> {
            CacheAnnotationProcessor processor = new CacheAnnotationProcessor();
            ReflectionTestUtils.setField(processor, "basePackage", "com.example.noentities");
            register(processor, selected, annotation);
            if (problem.equals("duplicateTable")) register(processor, LongId.class, annotation("books", "other:#id"));
            if (problem.equals("overlap")) register(processor, LongId.class, annotation("archive", "book:archive:#id"));
            if (problem.equals("samePrefix")) register(processor, LongId.class, annotation("archive", "book:#id"));
            if (problem.equals("indexOverlapsOtherGroup")) {
                AutoCache archive = annotation("archive", "archive:#id");
                when(archive.indexKeys()).thenReturn(new String[]{"book:old:#id"});
                register(processor, LongId.class, archive);
            }
            return processor;
        }).run(ctx -> assertThat(ctx).hasFailed().getFailure().hasRootCauseInstanceOf(
                problem.equals("missingId") ? NoSuchFieldException.class : IllegalStateException.class));
    }

    @Test
    void scannedUserDeclaresEmailIndexInItsOwnNamespace() {
        var users = scanned().getConfig("users");
        assertThat(users.getIndexKeys()).containsExactly(
                new com.example.canalcache.model.AutoCacheConfig.IndexKey("user-email:#email", "email"));
        assertThat(users.allPrefixes()).containsExactly("user:", "user-email:");
        assertThat(users.indexKey("email").buildKey("a@x.com")).isEqualTo("user-email:a@x.com");
    }

    @Test
    void tableIdDatabaseColumnIsUsedForEvents() {
        var processor = new CacheAnnotationProcessor();
        register(processor, CustomId.class, annotation("books", "book:#id"));
        assertThat(processor.getConfig("books").getIdField()).isEqualTo("book_id");
        assertThat(processor.getConfig("books").getIdFieldType()).isEqualTo(Long.class);
    }

    private ApplicationContextRunner context() {
        return new ApplicationContextRunner()
                .withBean(CacheAnnotationProcessor.class, () -> {
                    var processor = new CacheAnnotationProcessor();
                    ReflectionTestUtils.setField(processor, "basePackage", "com.example.canalcache.entity");
                    return processor;
                }).withBean(UserMapper.class, () -> mock(UserMapper.class))
                .withBean(DepartmentMapper.class, () -> mock(DepartmentMapper.class))
                .withBean(CacheLoaderRegistry.class);
    }
    private CacheAnnotationProcessor scanned() {
        var processor = new CacheAnnotationProcessor();
        ReflectionTestUtils.setField(processor, "basePackage", "com.example.canalcache.entity");
        processor.init(); return processor;
    }
    private AutoCache annotation(String table, String template) {
        AutoCache result = mock(AutoCache.class);
        when(result.table()).thenReturn(table); when(result.keyTemplate()).thenReturn(template);
        when(result.expireSeconds()).thenReturn(60); when(result.maxSizeBytes()).thenReturn(1000);
        when(result.strategy()).thenReturn(CacheStrategy.DELETE_MODE);
        when(result.indexKeys()).thenReturn(new String[0]); return result;
    }
    private void register(CacheAnnotationProcessor processor, Class<?> entity, AutoCache annotation) {
        ReflectionTestUtils.invokeMethod(processor, "registerConfig", entity, annotation);
    }
    static class LongId { Long id; }
    static class MissingId { String name; }
    static class StringId { String id; }
    static class MultipleIds { @TableId Long one; @TableId Long two; }
    static class CustomId { @TableId("book_id") Long primaryKey; }
}
