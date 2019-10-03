package org.openspaces.pu.container.jee.jetty.session;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.transaction.manager.DistributedJiniTxManagerConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ProcessingUnitConfig {

    @Value("${spaceName}") String spaceName;

    /*
    @Bean
    public MyBean myBean() {
        System.out.println("ProcessingUnitConfig.myBean()");
        return new MyBean();
    }
*/
    @Bean
    public GigaSpace gigaSpace() {
        EmbeddedSpaceConfigurer configurer = new EmbeddedSpaceConfigurer(spaceName);
        return new GigaSpaceConfigurer(configurer)
                .transactionManager(transactionManager())
                .create();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        try {
            return new DistributedJiniTxManagerConfigurer().transactionManager();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
