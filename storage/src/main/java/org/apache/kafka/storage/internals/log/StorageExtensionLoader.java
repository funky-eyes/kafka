/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Optional;

/** Loads an optional broker storage extension without coupling Kafka core to a concrete storage engine. */
public final class StorageExtensionLoader {
    public static final String STORAGE_EXTENSION_CLASS_CONFIG = "storage.extension.class";

    private StorageExtensionLoader() {
    }

    public static Optional<KafkaStorageExtension> load(StorageExtensionContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        Object configuredClass = context.originals().get(STORAGE_EXTENSION_CLASS_CONFIG);
        if (configuredClass == null || configuredClass.toString().isBlank()) {
            return Optional.empty();
        }

        KafkaStorageExtension extension = instantiate(configuredClass);
        try {
            extension.start(context);
            Objects.requireNonNull(extension.unifiedLogFactory(), "storage extension unifiedLogFactory");
            return Optional.of(extension);
        } catch (Throwable t) {
            try {
                extension.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            if (t instanceof IOException ioException) {
                throw ioException;
            }
            if (t instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (t instanceof Error error) {
                throw error;
            }
            throw new IOException("Failed to start storage extension " + configuredClass, t);
        }
    }

    private static KafkaStorageExtension instantiate(Object configuredClass) throws IOException {
        Class<?> extensionClass;
        try {
            extensionClass = configuredClass instanceof Class<?> clazz
                ? clazz
                : Class.forName(configuredClass.toString().trim(), true, contextClassLoader());
            Class<? extends KafkaStorageExtension> typedClass = extensionClass.asSubclass(KafkaStorageExtension.class);
            return typedClass.getDeclaredConstructor().newInstance();
        } catch (ClassCastException | ClassNotFoundException | NoSuchMethodException | InstantiationException |
                 IllegalAccessException | InvocationTargetException e) {
            throw new IOException("Unable to instantiate storage extension " + configuredClass, e);
        }
    }

    private static ClassLoader contextClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader != null ? classLoader : StorageExtensionLoader.class.getClassLoader();
    }
}
