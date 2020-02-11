package travel.ferries2.springdata.ignite;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.springdata20.repository.support.IgniteRepositoryImpl;
import org.jetbrains.annotations.NotNull;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.util.AnnotationDetectionFieldCallback;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;

import javax.cache.Cache;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;
import static org.apache.ignite.springdata20.repository.query.IgniteQueryGenerator.addSorting;
import static travel.ferries2.springdata.ignite.IgniteSqlQuery.JoinOperator.OR;
import static travel.ferries2.springdata.ignite.IterablesSupport.noMoreThanOneElement;


public class ExtendedIgniteRepositoryImplementation<T, ID extends Serializable> extends IgniteRepositoryImpl<T, ID> implements ExtendedIgniteRepository<T, ID> {
    private final EntityInformation<T, ID> entityInformation;
    @Getter
    private final IgniteCache<ID, T> cache;
    private final Field idField;
    private final List<Field> indexedIdentifiersFields;
    private final List<List<Field>> indexedCompositeIdentifiersFields;

    public ExtendedIgniteRepositoryImplementation(IgniteCache<ID, T> cache, EntityInformation<T, ID> entityInformation) {
        super(cache);
        this.entityInformation = entityInformation;
        this.cache = cache;

        AnnotationDetectionFieldCallback callback = new AnnotationDetectionFieldCallback(Id.class);
        ReflectionUtils.doWithFields(entityInformation.getJavaType(), callback);
        idField = callback.getRequiredField();
        ReflectionUtils.makeAccessible(idField);

        List<Field> indexedFields = Arrays.stream(entityInformation.getJavaType().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(QuerySqlField.class))
                .filter(field -> field.getAnnotation(QuerySqlField.class).index())
                .peek(field -> field.setAccessible(true))
                .collect(toList());
        indexedIdentifiersFields = indexedFields.stream()
                .filter(field -> field.isAnnotationPresent(Unique.class))
                .collect(toList());
        indexedCompositeIdentifiersFields = ImmutableList.copyOf(indexedFields.stream()
                .filter(field -> field.isAnnotationPresent(UniqueComposite.class))
                .collect(groupingBy(field -> field.getAnnotation(UniqueComposite.class).keyName()))
                .values());
    }

    @NotNull
    @Override
    @Transactional
    public <S extends T> S save(@NotNull S entity) {
        return save(getId(entity), entity);
    }

    @Override
    @Transactional
    public <S extends T> S save(ID key, S entity) {
        return findByUniqueFields(entity)
                .map(e -> super.save(getRequiredId(e), setId(key, getRequiredId(e), entity)))
                .orElseGet(() -> super.save(key, entity));
    }

    private <S extends T> S setId(ID key, ID id, S entity) {
        if (isNull(key)) {
            ReflectionUtils.setField(idField, entity, id);
        } else if (!key.equals(id)) {
            throw new DataIntegrityViolationException("Stored id:" + id + " does not correspond with new id:" + key);
        }
        return entity;
    }

    @Override
    @Transactional
    public <S extends T> Iterable<S> save(Map<ID, S> entities) {
        assertUniqueSecondaryKeys(entities);
        return super.save(entities.entrySet().stream()
                .map(entry -> findByUniqueFields(entry.getValue())
                        .map(e -> {
                            ID id = getRequiredId(e);
                            return (Map.Entry<ID, S>) Pair.of(id, setId(entry.getKey(), id, entry.getValue()));
                        }).orElse(entry))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private <S extends T> void assertUniqueSecondaryKeys(Map<ID, S> entities) {
        List<Object> identifiers = entities.values().stream()
                .flatMap(v -> Stream.concat(getExistingIndexedIdentifiers(v).stream(),
                        getCompositeIdentifiers(v).stream()))
                .collect(toList());
        if (identifiers.stream().collect(toUnmodifiableSet()).size() != identifiers.size()) {
            throw new DuplicateKeyException("Some elements share unique keys");
        }
    }

    @NotNull
    @Override
    @Transactional
    public <S extends T> Iterable<S> saveAll(@NotNull Iterable<S> entities) {
        return save(StreamsSupport.sequentialStream(entities)
                .collect(toMap(this::getRequiredId, Function.identity())));
    }

    @Override
    public void delete(@NotNull T entity) {
        findByUniqueIdentifiers(entity)
                .map(this::getRequiredId)
                .ifPresent(this::deleteById);
    }

    @NotNull
    @Override
    public Iterable<T> findAll(@NotNull Sort sort) {
        return () -> getAll(new SqlQuery<>(
                entityInformation.getJavaType(),
                addSorting(new StringBuilder(), sort).toString()))
                .iterator();
    }

    @Override
    public void deleteAll(@NotNull Iterable<? extends T> entities) {
        deleteAllById(StreamsSupport.sequentialStream(entities)
                .map(this::getRequiredId)
                .collect(toSet()));
    }

    @NotNull
    @Override
    public Page<T> findAll(@NotNull Pageable pageable) {
        if (pageable.isUnpaged()) {
            throw new IllegalArgumentException();
        }
        List<T> result = getAll(IgniteSqlQuery.pagination(entityInformation.getJavaType(), pageable)).collect(toList());
        return new PageImpl<>(
                result,
                pageable,
                result.size() < pageable.getPageSize()
                        ? result.size() + pageable.getOffset()
                        : count());
    }

    private Page<T> getPage(Pageable pageable, SqlQuery<ID, T> selectQuery, List<T> result) {
        if (pageable.isPaged()) {
            return new PageImpl<>(
                    result,
                    pageable,
                    result.size() < pageable.getPageSize()
                            ? result.size() + pageable.getOffset()
                            : count(selectQuery));
        }
        return new PageImpl<>(result);
    }


    @Override
    public Page<T> query(IgniteSqlQuery<T> query) {
        SqlQuery<ID, T> sqlQuery = query.toSqlQuery();

        List<T> result = getAll(sqlQuery).collect(toUnmodifiableList());

        return getPage(query.getPageable(), sqlQuery, result);
    }

    private Long count(SqlQuery<ID, T> selectQuery) {
        return (Long) noMoreThanOneElement(cache.query(IgniteSqlQuery.convertToCount(selectQuery))
                .getAll()).orElseThrow(() -> new EmptyResultDataAccessException("Expected count result", 1))
                .get(0);
    }

    @Override
    public Optional<T> findByUniqueIdentifiers(T entity) {
        return Optional.ofNullable(getId(entity))
                .flatMap(this::findById)
                .or(() -> findByUniqueFields(entity));
    }

    private Optional<T> findByUniqueFields(T entity) {
        return hasSecondaryIdentifiers() ? getBySecondaryIdentifiers(entity) : Optional.empty();
    }

    private <S extends T> ID getRequiredId(S entity) {
        return entityInformation.getRequiredId(entity);
    }

    private <S extends T> ID getId(S entity) {
        return entityInformation.getId(entity);
    }

    private Stream<T> getAll(SqlQuery<ID, T> sqlQuery) {
        return cache.query(sqlQuery).getAll().stream()
                .map(Cache.Entry::getValue);
    }

    private <S extends T> Optional<T> getBySecondaryIdentifiers(S entity) {

        List<Pair<String, ?>> identifiers = getExistingIndexedIdentifiers(entity);

        List<List<Pair<String, ?>>> compositeIdentifiers = getCompositeIdentifiers(entity);

        if (isNotEmpty(identifiers) || isNotEmpty(compositeIdentifiers)) {

            return noMoreThanOneElement(cache.query(IgniteSqlQuery.<T>builder()
                    .clazz(entityInformation.getJavaType())
                    .likeAndGroups(compositeIdentifiers)
                    .like(identifiers)
                    .joinOperator(OR)
                    .toSqlQuery())
                    .getAll())
                    .map(Cache.Entry::getValue);
        }

        return Optional.empty();
    }

    private boolean hasSecondaryIdentifiers() {
        return isEmpty(indexedIdentifiersFields) && isNotEmpty(indexedCompositeIdentifiersFields);
    }

    private List<List<Pair<String, ?>>> getCompositeIdentifiers(T entity) {
        return indexedCompositeIdentifiersFields.stream()
                .map(compositeId -> getIndexedFields(compositeId, entity))
                .collect(toUnmodifiableList());
    }

    private List<Pair<String, ?>> getIndexedFields(List<Field> fields, T entity) {
        return streamKeyValueFields(fields, entity)
                .collect(toUnmodifiableList());
    }

    private List<Pair<String, ?>> getExistingIndexedIdentifiers(T entity) {
        return streamKeyValueFields(indexedIdentifiersFields, entity)
                .filter(pair -> nonNull(pair.getValue()))
                .collect(toUnmodifiableList());
    }

    private Stream<Pair<String, ?>> streamKeyValueFields(List<Field> fields, T entity) {
        return fields.stream()
                .map(field -> getKeyValue(entity, field));
    }

    @SneakyThrows
    private Pair<String, ?> getKeyValue(T entity, Field field) {
        return Pair.of(field.getName(), field.get(entity));
    }

}
