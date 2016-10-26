package org.openspaces.fts;

import com.gigaspaces.query.extension.SpaceQueryExtension;
import org.openspaces.fts.spi.LuceneTextSearchQueryExtensionProvider;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author Vitaliy_Zinchenko
 */
@Target({METHOD, TYPE})
@Retention(RUNTIME)
public @interface SpaceTextAnalyzer {

    Class clazz();

}
