/*
 * Copyright (C) 2022-2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.sermant.xds.traffic.management.handler;

import io.sermant.core.service.xds.entity.XdsRetryPolicy;
import io.sermant.xds.common.flowcontrol.retry.Retry;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * default exception predicate creator
 *
 * @author zhouss
 * @since 2022-04-11
 */
public class DefaultRetryPredicateCreator implements RetryPredicateCreator {
    /**
     * default retry status code
     */
    private static final Collection<String> DEFAULT_RETRY_ON_RESPONSE_STATUS = Arrays.asList("502", "503");

    /**
     * default retry exception
     */
    private static final List<Class<? extends Throwable>> STRICT_RETRYABLE = Collections.unmodifiableList(
            Arrays.asList(ConnectException.class, SocketTimeoutException.class, IOException.class,
                    NoRouteToHostException.class)
    );

    @Override
    public Predicate<Throwable> createExceptionPredicate(Retry retry, XdsRetryPolicy policy) {
        return (Throwable ex) -> retry.isNeedRetry(ex, policy);
    }

    @Override
    public Predicate<Object> createResultPredicate(Retry retry, XdsRetryPolicy xdsRetryPolicy) {
        return result -> retry.isNeedRetry(result, xdsRetryPolicy);
    }
}
