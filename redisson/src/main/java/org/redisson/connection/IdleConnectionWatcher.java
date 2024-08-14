/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.connection;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisPubSubConnection;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.misc.AsyncSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IdleConnectionWatcher {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static class Entry {

        private final int minimumAmount;
        private final int maximumAmount;

        private final ConnectionsHolder<? extends RedisConnection> holder;
        private final AsyncSemaphore freeConnectionsCounter;
        private final Collection<? extends RedisConnection> connections;

        public Entry(int minimumAmount, int maximumAmount, ConnectionsHolder<? extends RedisConnection> holder) {
            super();
            this.minimumAmount = minimumAmount;
            this.maximumAmount = maximumAmount;
            // 空闲连接
            this.connections = holder.getFreeConnections();
            // 空闲连接的Semaphore
            this.freeConnectionsCounter = holder.getFreeConnectionsCounter();
            this.holder = holder;
        }

    };

    private final Map<ClientConnectionsEntry, List<Entry>> entries = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> monitorFuture;

    public IdleConnectionWatcher(EventLoopGroup group, MasterSlaveServersConfig config) {
        // 按照空闲连接超时时间作为检测的时间间隔
        monitorFuture = group.scheduleWithFixedDelay(() -> {
            long currTime = System.nanoTime();
            for (Entry entry : entries.values().stream().flatMap(m -> m.stream()).collect(Collectors.toList())) {
                if (!validateAmount(entry)) {
                    continue;
                }

                for (RedisConnection c : entry.connections) {
                    long timeInPool = TimeUnit.NANOSECONDS.toMillis(currTime - c.getLastUsageTime());

                    if (c instanceof RedisPubSubConnection
                            && (!((RedisPubSubConnection) c).getChannels().isEmpty()
                                    || !((RedisPubSubConnection) c).getPatternChannels().isEmpty()
                                        || !((RedisPubSubConnection) c).getShardedChannels().isEmpty())) {
                        continue;
                    }

                    if (timeInPool > config.getIdleConnectionTimeout()
                            && validateAmount(entry)
                                && entry.holder.remove(c)) {
                        ChannelFuture future = c.closeIdleAsync();
                        future.addListener((FutureListener<Void>) f ->
                                log.debug("Connection {} has been closed due to idle timeout. Not used for {} ms", c.getChannel(), timeInPool));
                    }
                }
            }
        }, config.getIdleConnectionTimeout(), config.getIdleConnectionTimeout(), TimeUnit.MILLISECONDS);
    }

    private boolean validateAmount(Entry entry) {
        /*
            1. freeConnectionsCounter 表示空闲的连接数量，这部分数量表示的连接可能从来没有创建过，也可能在freeConnections中
            2. entry.maximumAmount - entry.freeConnectionsCounter.getCounter() = 活跃连接的数量
            3. entry.connections.size() = freeConnections
            4. 为什么不只计算freeConnections的数量呢？因为需要判断当前创建的线程是否超过了minimumAmount，如果超过了需要进行空闲连接的移除
            5. 为什么不使用allConnections.size计算呢？可能的考虑是并发性能，因为计算queue的数量需要遍历，性能较差
         */
        return entry.maximumAmount - entry.freeConnectionsCounter.getCounter() + entry.connections.size() > entry.minimumAmount;
    }

    public void remove(ClientConnectionsEntry entry) {
        entries.remove(entry);
    }

    public void add(ClientConnectionsEntry entry, int minimumAmount, int maximumAmount, ConnectionsHolder<? extends RedisConnection> holder) {
        List<Entry> list = entries.computeIfAbsent(entry, k -> new ArrayList<>(2));
        list.add(new Entry(minimumAmount, maximumAmount, holder));
    }
    
    public void stop() {
        if (monitorFuture != null) {
            monitorFuture.cancel(true);
        }
    }

}
