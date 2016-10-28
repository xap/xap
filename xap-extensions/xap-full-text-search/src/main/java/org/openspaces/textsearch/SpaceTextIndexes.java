package org.openspaces.textsearch;

import com.gigaspaces.query.extension.SpaceQueryExtension;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author Vitaliy_Zinchenko
 */
@SpaceQueryExtension(providerClass = LuceneTextSearchQueryExtensionProvider.class)
@Target(METHOD)
@Retention(RUNTIME)
public @interface SpaceTextIndexes {

    SpaceTextIndex[] value();

}
