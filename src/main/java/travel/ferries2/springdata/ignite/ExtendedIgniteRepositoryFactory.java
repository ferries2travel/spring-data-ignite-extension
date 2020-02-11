package travel.ferries2.springdata.ignite;

import lombok.SneakyThrows;
import org.apache.ignite.Ignite;
import org.apache.ignite.springdata20.repository.support.IgniteRepositoryFactory;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReflectionEntityInformation;

import java.lang.reflect.Field;
import java.util.Map;

public class ExtendedIgniteRepositoryFactory extends IgniteRepositoryFactory {
    private final Map<Class<?>, String> repoToCache;
    private final Ignite ignite;

    public ExtendedIgniteRepositoryFactory(Ignite ignite) {
        super(ignite);
        this.ignite = ignite;
        this.repoToCache = getRepoToCache();
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private Map<Class<?>, String> getRepoToCache() {
        Field repoToCache = IgniteRepositoryFactory.class.getDeclaredField("repoToCache");
        repoToCache.setAccessible(true);
        return (Map<Class<?>, String>) repoToCache.get(this);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return ExtendedIgniteRepositoryImplementation.class;
    }

    @Override
    public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        return new ReflectionEntityInformation<>(domainClass);
    }

    @Override
    protected Object getTargetRepository(RepositoryInformation metadata) {
        return getTargetRepositoryViaReflection(
                metadata,
                ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())),
                getEntityInformation(metadata.getDomainType()));
    }
}






