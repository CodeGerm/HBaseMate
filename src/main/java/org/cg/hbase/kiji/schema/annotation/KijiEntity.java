package org.cg.hbase.kiji.schema.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A annotation to specify a Kiji Hbase entity
 * @author WZ
 *
 */
@Target(TYPE) 
@Retention(RUNTIME)
public @interface KijiEntity {
	
	/**
     * The name of the table.
     * <p> Defaults to the entity name.
     */
    String name();
    
    /**
     * The name of the column family.
     * <p> Defaults to the entity name.
     */
	String family() default "";
}
