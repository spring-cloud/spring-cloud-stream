package org.springframework.cloud.bus.jackson;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author Spencer Gibb
 */
public class SubtypeModule extends SimpleModule {
    private Class<?>[] subtypes;

    public SubtypeModule(Class<?>... subtypes) {
        this.subtypes = subtypes;
    }

    @Override
    public void setupModule(SetupContext context) {
        context.registerSubtypes(subtypes);
        super.setupModule(context);
    }
}
