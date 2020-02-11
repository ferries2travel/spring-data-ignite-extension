package travel.ferries2.springdata.ignite;

import lombok.Builder;
import lombok.Data;
import lombok.With;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.repository.core.EntityInformation;

import javax.cache.Cache;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtendedIgniteRepositoryImplementationTest {
    private static final Entity ENTITY = Entity.builder()
            .id("id")
            .field0(0)
            .field1("f1")
            .unique0("u0")
            .unique1("u1")
            .compositeUnique01("c01")
            .compositeUnique02("c02")
            .compositeUnique11("c11")
            .compositeUnique12("c12")
            .build();
    @Mock
    Ignite ignite;
    @Mock
    IgniteCache<String, Entity> igniteCache;
    @Mock
    EntityInformation<Entity, String> entityInformation;
    @Mock
    QueryCursor<Cache.Entry<String, Entity>> queryCursor;

    @Captor
    ArgumentCaptor<Entity> representationArgumentCaptor;
    @Captor
    ArgumentCaptor<SqlQuery<String, Entity>> queryArgumentCaptor;

    private ExtendedIgniteRepositoryImplementation<Entity, String> extendedIgniteRepositoryImplementation;

    @BeforeEach
    void setUp() {
        when(entityInformation.getJavaType()).thenReturn(Entity.class);
        extendedIgniteRepositoryImplementation = new ExtendedIgniteRepositoryImplementation<>(igniteCache, entityInformation);

    }

    private void mockEntityInformation() {
        when(entityInformation.getRequiredId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());
        when(entityInformation.getId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());
    }

    private void mockQuery() {
        when(igniteCache.query(any(Query.class))).thenReturn(queryCursor);
    }

    @Test
    void save() {
        when(entityInformation.getId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());
        mockQuery();
        when(queryCursor.getAll()).thenReturn(List.of());

        extendedIgniteRepositoryImplementation.save(ENTITY);

        verify(igniteCache).put(ENTITY.getId(), ENTITY);
    }

    @Test
    void replace() {
        mockEntityInformation();
        mockQuery();
        when(queryCursor.getAll()).thenReturn(List.of(new CacheEntryImpl<>(ENTITY.getId(), ENTITY)));

        Entity entity = ENTITY.toBuilder().id(null).field1("changed").build();
        extendedIgniteRepositoryImplementation.save(entity);

        verify(igniteCache).put(eq(ENTITY.getId()), representationArgumentCaptor.capture());

        assertThat(representationArgumentCaptor.getValue())
                .isEqualToComparingFieldByFieldRecursively(entity.withId(ENTITY.getId()));
    }

    @Test
    void dataIntegrityViolationException() {
        mockEntityInformation();
        mockQuery();
        when(queryCursor.getAll()).thenReturn(List.of(new CacheEntryImpl<>(ENTITY.getId(), ENTITY)));

        Entity entity = ENTITY.toBuilder().id("changedId").field1("changed").build();
        assertThrows(
                DataIntegrityViolationException.class,
                () -> extendedIgniteRepositoryImplementation.save(entity));
    }

    @Test
    void saveAll() {
        when(entityInformation.getRequiredId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());
        mockQuery();

        extendedIgniteRepositoryImplementation.saveAll(
                List.of(ENTITY, ENTITY.toBuilder()
                        .id("changed")
                        .unique0("changed")
                        .unique1("changed")
                        .compositeUnique02("changed")
                        .compositeUnique12("changed")
                        .build()));

        verify(igniteCache).putAll(anyMap());
    }

    @Test
    void saveAllUniqueViolation() {
        when(entityInformation.getRequiredId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());

        assertThrows(
                DuplicateKeyException.class,
                () -> extendedIgniteRepositoryImplementation.saveAll(
                        List.of(ENTITY, ENTITY.withId("changed"))));
    }

    @Test
    void saveAllCompositeUniqueViolation() {
        when(entityInformation.getRequiredId(any()))
                .thenAnswer(invocation -> invocation.getArgument(0, Entity.class).getId());

        assertThrows(
                DuplicateKeyException.class,
                () -> extendedIgniteRepositoryImplementation.saveAll(
                        List.of(ENTITY, ENTITY.toBuilder()
                                .id("changed")
                                .unique0("changed")
                                .unique1("changed")
                                .compositeUnique02("changed")
                                .build())));
    }

    @Test
    void delete() {
        mockEntityInformation();
        when(igniteCache.get(ENTITY.getId())).thenReturn(ENTITY);

        extendedIgniteRepositoryImplementation.delete(ENTITY);

        verify(igniteCache).remove(ENTITY.getId());
    }

    @Test
    void findAll() {
        mockQuery();
        when(queryCursor.getAll()).thenReturn(IntStream.range(0, 100)
                .mapToObj(i -> ENTITY.withId(String.valueOf(i)))
                .map(r -> new CacheEntryImpl<>(r.getId(), r))
                .collect(Collectors.toUnmodifiableList()));
        when(igniteCache.size(CachePeekMode.PRIMARY)).thenReturn(1000);

        Page<Entity> result = extendedIgniteRepositoryImplementation.findAll(PageRequest.of(1, 100));

        verify(igniteCache).query(queryArgumentCaptor.capture());
        verify(igniteCache).size(CachePeekMode.PRIMARY);

        assertThat(queryArgumentCaptor.getValue().getSql()).contains("LIMIT 100 OFFSET 100");
        assertThat(result.getTotalElements()).isEqualTo(1000);
        assertThat(result.getNumberOfElements()).isEqualTo(100);
    }


    @Data
    @With
    @Builder(toBuilder = true)
    private static class Entity {
        @Id
        private final String id;
        private final Integer field0;
        private final String field1;
        @Unique
        private final String unique0;
        @Unique
        private final String unique1;
        @UniqueComposite(keyName = "0")
        @QuerySqlField(index = true, groups = "0")
        private final String compositeUnique01;
        @UniqueComposite(keyName = "0")
        @QuerySqlField(index = true, groups = "0")
        private final String compositeUnique02;
        @UniqueComposite(keyName = "1")
        @QuerySqlField(index = true, groups = "1")
        private final String compositeUnique11;
        @UniqueComposite(keyName = "1")
        @QuerySqlField(index = true, groups = "1")
        private final String compositeUnique12;
    }
}