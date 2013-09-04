package org.infinispan.loaders.hbase;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.hbase.configuration.HBaseCacheStoreConfiguration;
import org.infinispan.loaders.hbase.configuration.HBaseCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.loaders.hbase.test.HBaseCluster;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.hbase.HBaseCacheStoreTest")
public class HBaseCacheStoreTest extends BaseCacheStoreTest {

   HBaseCluster hBaseCluster;

   @BeforeClass
   public void beforeClass() throws Exception {
      hBaseCluster = new HBaseCluster();
   }

   @AfterClass
   public void afterClass() throws CacheLoaderException {
      HBaseCluster.shutdown(hBaseCluster);
   }

   @Override
   protected CacheStore createCacheStore() throws Exception {
      HBaseCacheStore cs = new HBaseCacheStore();

      // This uses the default config settings in HBaseCacheStoreConfig
      ConfigurationBuilder cb = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);

      HBaseCacheStoreConfiguration storeConfiguration = cb.loaders()
            .addLoader(HBaseCacheStoreConfigurationBuilder.class)
               .purgeSynchronously(true)
               .hbaseZookeeperClientPort(hBaseCluster.getZooKeeperPort())
               .create();

      cs.init(storeConfiguration, getCache(), getMarshaller());
      cs.start();
      return cs;
   }

}
