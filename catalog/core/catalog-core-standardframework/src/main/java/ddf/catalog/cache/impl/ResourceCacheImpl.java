/**
 * Copyright (c) Codice Foundation
 * <p>
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 */
package ddf.catalog.cache.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.PassThroughMergePolicy;

import ddf.catalog.cache.ResourceCacheInterface;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.MetacardImpl;
import ddf.catalog.resource.Resource;
import ddf.catalog.resource.data.ReliableResource;

public class ResourceCacheImpl implements ResourceCacheInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceCacheImpl.class);

    private static final String PRODUCT_CACHE_NAME = "Product_Cache";

    private static final Long DEFAULT_MAX_CACHE_DIR_SIZE_MBYTES = 10240L;  //10 GB

    private static final long BYTES_IN_MEGABYTES = FileUtils.ONE_MB;

    private List<String> pendingCache = new ArrayList<>();

    private String productCacheDirectory;

    private HazelcastInstance instance;

    private IMap<Object, Object> cache;

    private FileSystemPersistenceProvider persistenceProvider;

    private ProductCacheDirListener<Object, Object> cacheListener;

    public ResourceCacheImpl(String productCacheDirectory, Long cacheDirMaxSizeMegabytes) {
        this.productCacheDirectory = productCacheDirectory;

        this.cacheListener = (cacheDirMaxSizeMegabytes != null) ?
                new ProductCacheDirListener<>(cacheDirMaxSizeMegabytes * BYTES_IN_MEGABYTES) :
                new ProductCacheDirListener<>(DEFAULT_MAX_CACHE_DIR_SIZE_MBYTES * BYTES_IN_MEGABYTES);

        this.persistenceProvider = new FileSystemPersistenceProvider(PRODUCT_CACHE_NAME, productCacheDirectory);

        this.instance = Hazelcast.newHazelcastInstance(initHazelConfig());

        this.instance.getMap(PRODUCT_CACHE_NAME).addEntryListener(cacheListener, "MAX_SIZE_LISTENER", true);
        this.cache = this.instance.getMap(PRODUCT_CACHE_NAME);
    }

    public void teardownCache() {
        instance.shutdown();
    }

    public long getCacheDirMaxSizeMegabytes() {
        LOGGER.debug("Getting max size for cache directory.");
        return cacheListener.getMaxDirSizeBytes() / BYTES_IN_MEGABYTES;
    }

    public void setCacheDirMaxSizeMegabytes(long cacheDirMaxSizeMegabytes) {
        LOGGER.debug("Setting max size for cache directory: {}", cacheDirMaxSizeMegabytes);
        cacheListener.setMaxDirSizeBytes(cacheDirMaxSizeMegabytes * BYTES_IN_MEGABYTES);
    }

    public String getProductCacheDirectory() {
        return productCacheDirectory;
    }

    public void setProductCacheDirectory(final String productCacheDirectory) {
        if ((productCacheDirectory == null) || productCacheDirectory.equals("")) {
            LOGGER.debug("Invalid product directory, keeping: {}", this.productCacheDirectory);
        } else {
            this.productCacheDirectory = productCacheDirectory;
            this.persistenceProvider.setPersistencePath(this.productCacheDirectory);
            createNewCache();
            LOGGER.debug("Set product cache directory to: {}", this.productCacheDirectory);
        }
    }

    /**
     * Returns true if resource with specified cache key is already in the process of
     * being cached. This check helps clients prevent attempting to cache the same resource
     * multiple times.
     *
     * @param key
     * @return
     */
    @Override
    public boolean isPending(String key) {
        return pendingCache.contains(key);
    }

    /**
     * Called by ReliableResourceDownloadManager when resource has completed being
     * cached to disk and is ready to be added to the cache map.
     *
     * @param reliableResource the resource to add to the cache map
     */
    @Override
    public void put(ReliableResource reliableResource) {
        LOGGER.trace("ENTERING: put(ReliableResource)");
        reliableResource.setLastTouchedMillis(System.currentTimeMillis());
        cache.put(reliableResource.getKey(), reliableResource);
        removePendingCacheEntry(reliableResource.getKey());

        LOGGER.trace("EXITING: put(ReliableResource)");
    }

    @Override
    public void removePendingCacheEntry(String cacheKey) {
        if (!pendingCache.remove(cacheKey)) {
            LOGGER.debug("Did not find pending cache entry with key = {}", cacheKey);
        } else {
            LOGGER.debug("Removed pending cache entry with key = {}", cacheKey);
        }
    }

    @Override
    public void addPendingCacheEntry(ReliableResource reliableResource) {
        String cacheKey = reliableResource.getKey();
        if (isPending(cacheKey)) {
            LOGGER.debug("Cache entry with key = {} is already pending", cacheKey);
        } else if (containsValid(cacheKey, reliableResource.getMetacard())) {
            LOGGER.debug("Cache entry with key = {} is already in cache", cacheKey);
        } else {
            pendingCache.add(cacheKey);
        }
    }

    /**
     * @param key
     * @return Resource, {@code null} if not found.
     */
    @Override
    public Resource getValid(String key, Metacard latestMetacard) {
        LOGGER.trace("ENTERING: get()");
        if (key == null) {
            throw new IllegalArgumentException("Must specify non-null key");
        }
        if (latestMetacard == null) {
            throw new IllegalArgumentException("Must specify non-null metacard");
        }
        LOGGER.debug("key {}", key);

        ReliableResource cachedResource = (ReliableResource) cache.get(key);

        // Check that ReliableResource actually maps to a file (product) in the
        // product cache directory. This check handles the case if the product
        // cache directory has had files deleted from it.
        if (cachedResource != null) {
            if (!validateCacheEntry(cachedResource, latestMetacard)) {
                LOGGER.debug(
                        "Entry found in cache was out-of-date or otherwise invalid.  Will need to be re-cached.  Entry key: {} "
                                + key);
                return null;
            }

            if (cachedResource.hasProduct()) {
                LOGGER.trace("EXITING: get() for key {}", key);
                return cachedResource;
            } else {
                cache.remove(key);
                LOGGER.debug(
                        "Entry found in the cache, but no product found in cache directory for key = {} "
                                + key);
                return null;
            }
        } else {
            LOGGER.debug("No product found in cache for key = {}", key);
            return null;
        }

    }

    /**
     * States whether an item is in the cache or not.
     *
     * @param key
     * @return {@code true} if items exists in cache.
     */
    @Override
    public boolean containsValid(String key, Metacard latestMetacard) {
        if (key == null) {
            return false;
        }
        ReliableResource cachedResource = (ReliableResource) cache.get(key);
        return (cachedResource != null) && (validateCacheEntry(cachedResource, latestMetacard));
    }

    /**
     * Compares the {@link Metacard} in a {@link ReliableResource} pulled from cache with a Metacard obtained directly
     * from the Catalog to ensure they are the same. Typically used to determine if the cache entry is out-of-date based
     * on the Catalog having an updated Metacard.
     *
     * @param cachedResource
     * @param latestMetacard
     * @return true if the cached ReliableResource still matches the most recent Metacard from the Catalog, false otherwise
     */
    protected boolean validateCacheEntry(ReliableResource cachedResource, Metacard latestMetacard) {
        LOGGER.trace("ENTERING: validateCacheEntry");
        if (cachedResource == null || latestMetacard == null) {
            throw new IllegalArgumentException(
                    "Neither the cachedResource nor the metacard retrieved from the catalog can be null.");
        }

        int cachedResourceHash = cachedResource.getMetacard()
                .hashCode();
        MetacardImpl latestMetacardImpl = new MetacardImpl(latestMetacard);
        int latestMetacardHash = latestMetacardImpl.hashCode();

        // compare hashes of cachedResource.getMetacard() and latestMetcard
        if (cachedResourceHash == latestMetacardHash) {
            LOGGER.trace("EXITING: validateCacheEntry");
            return true;
        } else {
            File cachedFile = new File(cachedResource.getFilePath());
            if (!FileUtils.deleteQuietly(cachedFile)) {
                LOGGER.debug("File was not removed from cache directory.  File Path: {}",
                        cachedResource.getFilePath());
            }

            cache.remove(cachedResource.getKey());
            LOGGER.trace("EXITING: validateCacheEntry");
            return false;
        }
    }

    private void createNewCache() {
        File directory = new File(FilenameUtils.normalize(productCacheDirectory));

        // Create the directory if it doesn't exist
        if ((!directory.exists() && directory.mkdirs()) || (directory.isDirectory()
                && directory.canRead() && directory.canWrite())) {
            LOGGER.debug("Setting product cache directory to: {}", productCacheDirectory);
        }
    }

    private Config initHazelConfig() {
        Config cfg = new Config();

        cfg.setInstanceName(PRODUCT_CACHE_NAME);

        NetworkConfig network = cfg.getNetworkConfig();
        network.setPortAutoIncrement(true);
        network.setPort(5701);
        List<Integer> outboundPorts = new ArrayList<>();
        outboundPorts.add(0);
        network.setOutboundPorts(outboundPorts);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAwsConfig().setEnabled(false);
        network.getInterfaces().setEnabled(false);
        network.setSSLConfig(new SSLConfig().setEnabled(false));
        network.setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(false));
        network.setSymmetricEncryptionConfig(new SymmetricEncryptionConfig().setEnabled(false));

        cfg.setPartitionGroupConfig(new PartitionGroupConfig().setEnabled(false));

        ExecutorConfig defaultExecutorConfig = new ExecutorConfig();
        defaultExecutorConfig.setName("default");
        defaultExecutorConfig.setPoolSize(16);
        defaultExecutorConfig.setQueueCapacity(0);

        cfg.addExecutorConfig(defaultExecutorConfig);

        QueueConfig defaultQueueConfig = new QueueConfig();
        defaultQueueConfig.setName("default");
        defaultQueueConfig.setMaxSize(0);
        defaultQueueConfig.setBackupCount(1);
        defaultQueueConfig.setAsyncBackupCount(0);
        defaultQueueConfig.setEmptyQueueTtl(-1);

        cfg.addQueueConfig(defaultQueueConfig);

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("default");
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setBackupCount(2);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setTimeToLiveSeconds(0);
        mapConfig.setMaxIdleSeconds(0);
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mapConfig.getMaxSizeConfig().setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE).setSize(0);
        mapConfig.setEvictionPercentage(25);
        mapConfig.setMergePolicy(PassThroughMergePolicy.class.getCanonicalName());

        MapConfig cacheMap = new MapConfig();
        cacheMap.setName(PRODUCT_CACHE_NAME);
        cacheMap.setInMemoryFormat(InMemoryFormat.BINARY);
        cacheMap.setBackupCount(0);
        cacheMap.setAsyncBackupCount(0);
        cacheMap.setTimeToLiveSeconds(0);
        cacheMap.setMaxIdleSeconds(0);
        cacheMap.setEvictionPolicy(MapConfig.EvictionPolicy.NONE);
        cacheMap.getMaxSizeConfig().setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE).setSize(0);
        cacheMap.setEvictionPercentage(25);
        cacheMap.setMergePolicy(PassThroughMergePolicy.class.getCanonicalName());
        cacheMap.setMapStoreConfig(new MapStoreConfig().setEnabled(true));

        cacheMap.getMapStoreConfig().setImplementation(persistenceProvider);
//        cacheMap.getMapStoreConfig().setFactoryClassName(FileSystemMapStoreFactory.class.getCanonicalName());
        cacheMap.getMapStoreConfig().setWriteDelaySeconds(0);

        cfg.addMapConfig(mapConfig);
        cfg.addMapConfig(cacheMap);

        MultiMapConfig defaultMultiMapConfig = new MultiMapConfig();
        defaultMultiMapConfig.setName("default");
        defaultMultiMapConfig.setBackupCount(1);
        defaultMultiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);

        cfg.addMultiMapConfig(defaultMultiMapConfig);

        ListConfig defaultListConfig = new ListConfig();
        defaultListConfig.setName("default");
        defaultListConfig.setBackupCount(1);

        cfg.addListConfig(defaultListConfig);

        SetConfig defaultSetConfig = new SetConfig();
        defaultSetConfig.setName("default");
        defaultSetConfig.setBackupCount(1);

        cfg.addSetConfig(defaultSetConfig);

        SemaphoreConfig defaultSemaphoreConfig = new SemaphoreConfig();
        defaultSemaphoreConfig.setName("default");
        defaultSemaphoreConfig.setInitialPermits(0);
        defaultSemaphoreConfig.setBackupCount(1);
        defaultSemaphoreConfig.setAsyncBackupCount(0);

        cfg.addSemaphoreConfig(defaultSemaphoreConfig);

        cfg.getSerializationConfig().setPortableVersion(0);
        cfg.getServicesConfig().setEnableDefaults(true);

        return cfg;
    }
}
