package org.cg.hbase.kiji.schema.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A annotation to specify a Hbase column
 * @author WZ
 *
 */
@Target(FIELD) 
@Retention(RUNTIME)
public @interface Column {
	
	/**
     * (Optional) The name of the column. Defaults to 
     * the property or field name.
     */
    String name() default "";
	/**
     * (Optional) The name of the column. Defaults to 
     * the family in KijiEntity.
     */
    String family() default "";
}
