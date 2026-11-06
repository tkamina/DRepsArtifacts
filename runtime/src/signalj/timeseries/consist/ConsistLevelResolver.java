package signalj.timeseries.consist;

import java.lang.reflect.AnnotatedElement;

import signalj.timeseries.annotation.causal;
import signalj.timeseries.annotation.eventual;
import signalj.timeseries.annotation.serializable;

public class ConsistLevelResolver {
	
public static ConsistLevel  resolve(AnnotatedElement element) { 
  if (element.isAnnotationPresent(serializable.class)) {
	  return ConsistLevel.serializable;
  }else if (element.isAnnotationPresent(causal.class)) {
	  return ConsistLevel.causal;
  }else if (element.isAnnotationPresent(eventual.class)) {
	  return ConsistLevel.eventual;
  }else {
	  return ConsistLevel.DEFAULT;
  }
}

}
