package org.springframework.cloud.bus.jackson;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Donovan Muller
 */
public class RemoteApplicationEventRegistrar implements ImportBeanDefinitionRegistrar {

    // patterned after Spring Integration IntegrationComponentScanRegistrar

    @Override
    public void registerBeanDefinitions(final AnnotationMetadata importingClassMetadata,
            final BeanDefinitionRegistry registry) {

        Map<String, Object> componentScan = importingClassMetadata
                .getAnnotationAttributes(RemoteApplicationEventScan.class.getName(), false);

        Set<String> basePackages = new HashSet<>();
        for (String pkg : (String[]) componentScan.get("value")) {
            if (StringUtils.hasText(pkg)) {
                basePackages.add(pkg);
            }
        }
        for (String pkg : (String[]) componentScan.get("basePackages")) {
            if (StringUtils.hasText(pkg)) {
                basePackages.add(pkg);
            }
        }
        for (Class<?> clazz : (Class[]) componentScan.get("basePackageClasses")) {
            basePackages.add(ClassUtils.getPackageName(clazz));
        }

        if (basePackages.isEmpty()) {
            basePackages.add(ClassUtils.getPackageName(importingClassMetadata.getClassName()));
        }

        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(BusJacksonMessageConverter.class);
        beanDefinitionBuilder.addPropertyValue("packagesToScan", basePackages.toArray(new String[basePackages.size()]));
        AbstractBeanDefinition beanDefinition = beanDefinitionBuilder.getBeanDefinition();

        BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition, "busJsonConverter");
        BeanDefinitionReaderUtils.registerBeanDefinition(holder, registry);
    }
}
