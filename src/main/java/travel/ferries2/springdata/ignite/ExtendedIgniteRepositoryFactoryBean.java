package travel.ferries2.springdata.ignite;

import org.apache.ignite.Ignite;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import java.io.Serializable;

public class ExtendedIgniteRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
        extends RepositoryFactoryBeanSupport<T, S, ID> implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    protected ExtendedIgniteRepositoryFactoryBean(Class<T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @NotNull
    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        return new ExtendedIgniteRepositoryFactory(applicationContext.getBean(Ignite.class));
    }
}
