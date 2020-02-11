package travel.ferries2.springdata.ignite;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.io.Serializable;
import java.util.Optional;

@NoRepositoryBean
public interface ExtendedIgniteRepository<T, ID extends Serializable> extends IgniteRepository<T, ID>, PagingAndSortingRepository<T, ID> {

    Optional<T> findByUniqueIdentifiers(T entity);

    Page<T> query(IgniteSqlQuery<T> query);

    IgniteCache<ID, T> getCache();
}