package travel.ferries2.springdata.ignite;

import com.google.common.collect.Iterables;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class IterablesSupport {

    static <T> Optional<T> noMoreThanOneElement(Iterable<T> iterable) {
        return Optional.ofNullable(Iterables.getOnlyElement(iterable, null));
    }
}
