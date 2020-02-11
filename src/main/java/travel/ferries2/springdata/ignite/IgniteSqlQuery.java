package travel.ferries2.springdata.ignite;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.springframework.data.domain.Pageable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.ListUtils.emptyIfNull;
import static org.apache.ignite.springdata20.repository.query.IgniteQueryGenerator.addPaging;

@Getter
@Builder
public class IgniteSqlQuery<T> {

    private final List<List<Pair<String, ?>>> likeAndGroups;
    private final List<Pair<String, ?>> mayorOrEqual;
    private final List<Pair<String, ?>> minorOrEqual;
    private final List<Pair<String, ?>> like;
    private final Pageable pageable;
    private final JoinOperator joinOperator;
    @NonNull
    private final Class<T> clazz;

    public static <ID extends Serializable, T> SqlFieldsQuery convertToCount(SqlQuery<ID, T> selectQuery) {
        return new SqlFieldsQuery("COUNT (*) " + deletePagination(selectQuery.getSql())).setArgs(selectQuery.getArgs());
    }

    static <ID, T> SqlQuery<ID, T> pagination(Class<T> type, Pageable pageable) {
        return create(type, pagination(pageable));
    }

    <ID> SqlQuery<ID, T> toSqlQuery() {
        return IgniteSqlQuery.<ID, T>create(clazz, StreamsSupport.concat(
                likeGroups(likeAndGroups),
                setOperator(mayorOrEqual, " >= ?"),
                setOperator(minorOrEqual, " <= ?"),
                like(like))
                .collect(joining(" " + joinOperator.name() + " "))
                + pagination(pageable))
                .setArgs(Stream.concat(likeAndGroups.stream(), Stream.of(mayorOrEqual, minorOrEqual, like))
                        .flatMap(List::stream)
                        .map(Pair::getValue)
                        .filter(Objects::nonNull)
                        .toArray());
    }

    private Stream<String> setOperator(List<Pair<String, ?>> fields, String operator) {
        return fields.stream().map(f -> f.getKey() + operator);
    }

    private Stream<String> likeGroups(List<List<Pair<String, ?>>> compositeIdentifiers) {
        return compositeIdentifiers.stream()
                .map(compositeId -> "( " + like(compositeId).collect(joining(" AND ")) + " )");
    }

    private Stream<String> like(List<Pair<String, ?>> identifiers) {
        return identifiers.stream()
                .map(field -> nonNull(field.getValue()) ? field.getKey() + " LIKE ?" : field.getKey() + " IS NULL");
    }

    public enum JoinOperator {
        AND, OR
    }


    private static <ID, T> SqlQuery<ID, T> create(Class<T> type, String sql) {
        return new SqlQuery<>(type, sql);
    }

    private static String pagination(Pageable pageable) {
        return Optional.ofNullable(pageable)
                .filter(Pageable::isPaged)
                .map(p -> addPaging(new StringBuilder(), p))
                .map(StringBuilder::toString)
                .orElse("");
    }

    private static String deletePagination(String sql) {
        int orderByIndex = sql.indexOf(" ORDER BY ");
        if (orderByIndex != -1) {
            return sql.substring(0, orderByIndex);
        }
        int limitIndex = sql.indexOf(" LIMIT ");
        if (limitIndex != -1) {
            return sql.substring(0, limitIndex);
        }
        return sql;
    }

    public static class IgniteSqlQueryBuilder<T> {
        public IgniteSqlQuery<T> build() {
            return new IgniteSqlQuery<>(
                    emptyIfNull(likeAndGroups),
                    emptyIfNull(mayorOrEqual),
                    emptyIfNull(minorOrEqual),
                    emptyIfNull(like),
                    Optional.ofNullable(pageable).orElse(Pageable.unpaged()),
                    Optional.ofNullable(joinOperator).orElse(JoinOperator.AND),
                    clazz);
        }

        public <ID> SqlQuery<ID, T> toSqlQuery() {
            return build().toSqlQuery();
        }
    }
}
