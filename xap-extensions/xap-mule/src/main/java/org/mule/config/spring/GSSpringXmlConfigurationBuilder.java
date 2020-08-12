package org.mule.config.spring;

import org.mule.api.config.ConfigurationException;
import org.mule.config.ConfigResource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GSSpringXmlConfigurationBuilder extends SpringXmlConfigurationBuilder {

    public static final String GS_MULE_DEFAULTS_CONFIG = "default-mule-config-gs.xml";
    public static final String GS_MULE_SPRING_CONFIG = "mule-spring-config-gs.xml";
    public static final String GS_MULE_MINIMAL_SPRING_CONFIG = "minimal-mule-config-gs.xml";
    public static final String GS_MULE_REGISTRY_BOOTSTRAP_SPRING_CONFIG = "registry-bootstrap-mule-config-gs.xml";
    public static final String GS_MULE_DOMAIN_REGISTRY_BOOTSTRAP_SPRING_CONFIG = "registry-bootstrap-mule-domain-config-gs.xml";

    public GSSpringXmlConfigurationBuilder(String configResources) throws ConfigurationException {
        super(configResources);
    }


    @Override
    protected void addResources(List<ConfigResource> allResources) {
        allResources.clear();
        try {

            if (useMinimalConfigResource)
            {
                allResources.add(new ConfigResource(GS_MULE_DOMAIN_REGISTRY_BOOTSTRAP_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_MINIMAL_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
            else if (useDefaultConfigResource)
            {
                allResources.add(new ConfigResource(GS_MULE_REGISTRY_BOOTSTRAP_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_MINIMAL_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.add( new ConfigResource(GS_MULE_DEFAULTS_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
            else
            {
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
        } catch (IOException e) {
            logger.error("Failed to add resources", e);
            throw new RuntimeException(e);
        }
    }
}
