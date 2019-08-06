package org.galatea.pochdfs.utils.benchmarking;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Aspect
@Component
@Slf4j
public class Logger {

	@Around("execution(* *(..)) && @annotation(org.galatea.pochdfs.utils.benchmarking.MethodStats)")
	public Object log(final ProceedingJoinPoint point) throws Throwable {
		long start = System.currentTimeMillis();
		Object result = point.proceed();
		log.info("className={}, methodName={}, timeMs={},threadId={}",
				new Object[] { MethodSignature.class.cast(point.getSignature()).getDeclaringTypeName(),
						MethodSignature.class.cast(point.getSignature()).getMethod().getName(),
						System.currentTimeMillis() - start, Thread.currentThread().getId() });
		return result;
	}
}
