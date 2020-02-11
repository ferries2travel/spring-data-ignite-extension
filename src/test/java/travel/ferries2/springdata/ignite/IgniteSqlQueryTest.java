package travel.ferries2.springdata.ignite;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static travel.ferries2.springdata.ignite.IgniteSqlQuery.JoinOperator.OR;

class IgniteSqlQueryTest {

    @ParameterizedTest
    @ValueSource(strings = {" ORDER BY field1 LIMIT 100 OFFSET 0", " LIMIT 100 OFFSET 0"})
    void convertToCount(String tail) {
        String fields = "field1 LIKE ?, field2 >= ?";
        Object[] arguments = {"1", "2"};
        assertThat(IgniteSqlQuery.convertToCount(new SqlQuery<String, Object>(Object.class, fields + tail)
                .setArgs(arguments)))
                .has(new Condition<>(sqlFieldsQuery -> sqlFieldsQuery.getSql().equals("COUNT (*) " + fields), null))
                .extracting(SqlFieldsQuery::getArgs)
                .isEqualTo(arguments);
    }

    @Test
    void pagination() {
        assertThat(IgniteSqlQuery.pagination(Object.class, PageRequest.of(1, 100, Sort.by("field1").descending()))
                .getSql()).isEqualToIgnoringCase(" ORDER BY field1 DESC LIMIT 100 OFFSET 100");
    }

    @Test
    void query() {
        SqlQuery<String, Object> sqlQuery = IgniteSqlQuery.builder().clazz(Object.class)
                .likeAndGroups(List.of(List.of(Pair.of("fieldGroup0", 1), Pair.of("fieldGroup1", null))))
                .mayorOrEqual(List.of(Pair.of("field0", 2)))
                .minorOrEqual(List.of(Pair.of("field1", 3)))
                .pageable(PageRequest.of(1, 200))
                .like(List.of(Pair.of("field2", null), Pair.of("field3", "value1")))
                .joinOperator(OR)
                .toSqlQuery();
        assertThat(sqlQuery.getSql())
                .isEqualToNormalizingWhitespace(
                        "( fieldGroup0 LIKE ? AND fieldGroup1 IS NULL )" +
                                " OR field0 >= ?" +
                                " OR field1 <= ?" +
                                " OR field2 IS NULL" +
                                " OR field3 LIKE ?" +
                                " LIMIT 200 OFFSET 200");
        assertThat(sqlQuery.getArgs()).containsExactly(1, 2, 3, "value1");

    }
}