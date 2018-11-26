package org.openspaces.core.space;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Hashtable;

public abstract class AbstractAnnotationRegistry {

    private Hashtable<Class<?>, HashSet<RegistryEntry>> registry = new Hashtable<Class<?>, HashSet<RegistryEntry>>();
    private Log logger = LogFactory.getLog(this.getClass());

    /**
     * Registers the bean as a listener for a event specified by the annotation. When an
     * event fires the specified bean method will be invoked.
     *
     * @param annotation The annotation that specifies the event the bean is registered to.
     * @param object     The bean instance.
     * @param method     The bean's method to invoke when the event fires.
     * @throws IllegalArgumentException When the specified method has invalid arguments or annotation
     */
    public void registerAnnotation(Class<?> annotation, Object object, Method method) throws IllegalArgumentException {
        // check that the parameters are non-null.
        if (annotation == null || object == null || method == null) {
            throw new IllegalArgumentException("Illegal null argument in parameter: " +
                    annotation == null ? "annotation" : object == null ? "object" : "method");
        }

        validateMethod(annotation, method); // this throws IllegalArgumentException if method not valid
        storeMethod(annotation, object, method);
    }

    // TODO: javadoc
    protected abstract void validateMethod(Class<?> annotation, Method method);

    /**
     * Invokes the registered beans' methods passing them the space mode change event.
     * @param annotationClass - the annotation class of the event
     * @param event   The event to pass to the methods in case they expect a parameter.
     */
    protected void fireEvent(Class<?> annotationClass, Object event) {
        HashSet<RegistryEntry> entries = registry.get(annotationClass);

        if (entries != null) {
            for (RegistryEntry registryEntry : entries) {
                try {
                    if (registryEntry.method.getParameterTypes().length == 0) {
                        registryEntry.method.invoke(registryEntry.object);
                    } else {
                        registryEntry.method.invoke(registryEntry.object, event);
                    }
                } catch (InvocationTargetException e) {
                    logger.error("Target invocation method threw an exception. Bean: " +
                            registryEntry.object + ", Method: " + registryEntry.method, e);
                } catch (Exception e) {
                    logger.error("Failed to invoke target invocation method. Bean: " +
                            registryEntry.object + ", Method: " + registryEntry.method, e);
                }
            }
        }
    }

    private void storeMethod(Class<?> annotation, Object object, Method method) {
        HashSet<RegistryEntry> methods = registry.get(annotation);
        if (methods == null) {
            methods = new HashSet<RegistryEntry>();
            registry.put(annotation, methods);
        }
        // add the entry to the registry
        RegistryEntry entry = new RegistryEntry(object, method);
        methods.add(entry);
    }

    /**
     * An entry in the registry that holds the bean instance and the method to invoke.
     */
    private static class RegistryEntry {

        /**
         * The bean instance.
         */
        Object object;

        /**
         * The method to invoke.
         */
        Method method;

        /**
         * Create a new {@link RegistryEntry} instance.
         *
         * @param object The bean instance.
         * @param method The method to invoke.
         */
        public RegistryEntry(Object object, Method method) {
            this.object = object;
            this.method = method;
        }

        @Override
        public boolean equals(Object o) {
            if (o != null && o instanceof RegistryEntry) {
                RegistryEntry entry = (RegistryEntry) o;
                // need to be the same object instance(!) and same method
                if (entry.object == this.object &&
                        entry.method.equals(method)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 1;
            hash = hash * 31 + object.hashCode();
            hash = hash * 31 + method.hashCode();
            return hash;
        }
    }

}