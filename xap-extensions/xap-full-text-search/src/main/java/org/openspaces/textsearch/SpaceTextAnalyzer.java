package org.openspaces.textsearch;

import com.gigaspaces.query.extension.SpaceQueryExtension;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author Vitaliy_Zinchenko
 */
@SpaceQueryExtension(providerClass = LuceneTextSearchQueryExtensionProvider.class)
@Target({METHOD, TYPE})
@Retention(RUNTIME)
public @interface SpaceTextAnalyzer {

    Class analyzer();

    String path() default "";

}
