package {{project.groupId}};

import java.util.Properties;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoContext;
import org.openspaces.core.config.annotation.EmbeddedSpaceBeansConfig;
import org.openspaces.core.space.EmbeddedSpaceFactoryBean;
import org.openspaces.persistency.hibernate.DefaultHibernateSpaceDataSourceConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;

public class CustomSpaceConfig extends EmbeddedSpaceBeansConfig {

	@ClusterInfoContext
	private ClusterInfo clusterInfo;

	@Value("${db.driver}")
	private String dbDriver;
	@Value("${db.url}")
	private String dbUrl;
	@Value("${db.user:#{null}}")
	private String dbUser;
	@Value("${db.password:#{null}}")
	private String dbPassword;
	@Value("${hibernate.dialect}")
	private String hibernateDialect;
	@Value("${hibernate.limitResults:-1}")
	private int limitResults;
	
    @Override
    protected void configure(EmbeddedSpaceFactoryBean factoryBean) {
		super.configure(factoryBean);

		factoryBean.setSchema("persistent");
		factoryBean.setMirrored(true);
        factoryBean.setSpaceDataSource(new DefaultHibernateSpaceDataSourceConfigurer()
		    .sessionFactory(initSessionFactory())
		    .clusterInfo(clusterInfo)
		    .limitResults(limitResults)
		    .create());

		Properties properties = new Properties();
        properties.setProperty("space-config.engine.cache_policy", "1"); // 1 == ALL IN CACHE
        properties.setProperty("space-config.external-data-source.usage", "read-only");
        properties.setProperty("cluster-config.cache-loader.external-data-source", "true");
        properties.setProperty("cluster-config.cache-loader.central-data-source", "true");
        factoryBean.setProperties(properties);
    }

	private SessionFactory initSessionFactory() {
		return new LocalSessionFactoryBuilder(initDataSource())
		    .scanPackages("{{project.groupId}}.model")
            .setProperty("hibernate.dialect", hibernateDialect)
            .setProperty("hibernate.cache.provider_class", "org.hibernate.cache.NoCacheProvider")
            .setProperty("hibernate.jdbc.use_scrollable_resultset", "true")
		    .buildSessionFactory();
	}
	
	private DataSource initDataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(dbDriver);
		dataSource.setUrl(dbUrl);
		dataSource.setUsername(dbUser);
		dataSource.setPassword(dbPassword);
		return dataSource;
	}
}
