package com.chinagoods.bigdata.connectors.http.internal.utils;

import java.lang.Thread.UncaughtExceptionHandler;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.apache.flink.util.ExceptionUtils.stringifyException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public final class ThreadUtils {

    public static final UncaughtExceptionHandler LOGGING_EXCEPTION_HANDLER =
        (t, e) -> log.warn("Thread:" + t + " exited with Exception:" + stringifyException(e));
}
