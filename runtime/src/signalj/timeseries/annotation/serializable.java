package signalj.timeseries.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//@Target({ElementType.METHOD, ElementType.FIELD})
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface serializable {
}
