/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.store.event;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An event bus that execute listenersByType in fixed order
 *
 * @author keli.wang
 * @since 2018/7/13
 */
public class FixedExecOrderEventBus {
    private final ReadWriteLock guard;
    private final ListMultimap<Class<?>, Listener> listenersByType;

    public FixedExecOrderEventBus() {
        this.guard = new ReentrantReadWriteLock();
        this.listenersByType = ArrayListMultimap.create();
    }

    public <E> void subscribe(final Class<E> clazz, final Listener<E> listener) {
        final Lock lock = guard.writeLock();
        lock.lock();
        try {
            listenersByType.put(clazz, listener);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public void post(final Object event) {
        final Lock lock = guard.readLock();

        lock.lock();

        try {
            final List<Listener> listeners = listenersByType.get(event.getClass());
            if (listeners == null) {
                throw new RuntimeException("unsupported event type " + event.getClass().getSimpleName());
            }

            for (final Listener listener : listeners) {
                listener.onEvent(event);
            }
        } finally {
            lock.unlock();
        }
    }

    public interface Listener<E> {
        void onEvent(final E event);
    }
}
