package org.cg.hbase.kiji.schema.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A annotation to specify a Kiji data translator
 * @author WZ
 *
 */
@Target(FIELD) 
@Retention(RUNTIME)
public @interface Translator {
	/**
     * The class of the translator class
     * <p> Defaults to the entity name.
     */
	Class<?> using();
}