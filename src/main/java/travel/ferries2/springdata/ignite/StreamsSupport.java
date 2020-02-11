package travel.ferries2.springdata.ignite;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class StreamsSupport {
    @SafeVarargs
    static <T> Stream<T> concat(Stream<T>... streams) {
        return Arrays.stream(streams)
                .flatMap(Function.identity());
    }

    static <T> Stream<T> sequentialStream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
