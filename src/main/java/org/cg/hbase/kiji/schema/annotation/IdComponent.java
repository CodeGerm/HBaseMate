package org.cg.hbase.kiji.schema.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A annotation to specify a Hbase entity id component
 * @author WZ
 *
 */
@Target(FIELD) 
@Retention(RUNTIME)
public @interface IdComponent {
	
	/**
     * (Optional) The name of the column. Defaults to 
     * the property or field name.
     */
    String name() default "";
}
