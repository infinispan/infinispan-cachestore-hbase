package org.infinispan.loaders.hbase.configuration;

import junit.framework.Assert;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.hbase.HBaseCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testHBaseCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(HBaseCacheStoreConfigurationBuilder.class)
         .autoCreateTable(false)
         .entryColumnFamily("ECF")
         .entryTable("ET")
         .entryValueField("EVF")
         .expirationColumnFamily("XCF")
         .expirationTable("XT")
         .expirationValueField("XVF")
         .hbaseZookeeperClientPort(4321)
         .hbaseZookeeperQuorumHost("myhost")
         .sharedTable(true)
      .fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      HBaseCacheStoreConfiguration store = (HBaseCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      Assert.assertFalse(store.autoCreateTable());
      Assert.assertTrue(store.entryColumnFamily().equals("ECF"));
      Assert.assertEquals("ET", store.entryTable());
      Assert.assertEquals("EVF", store.entryValueField());
      Assert.assertEquals("XT", store.expirationTable());
      Assert.assertEquals("XCF", store.expirationColumnFamily());
      Assert.assertEquals("XVF", store.expirationValueField());
      Assert.assertEquals("myhost", store.hbaseZookeeperQuorumHost());
      Assert.assertEquals(4321, store.hbaseZookeeperClientPort());
      Assert.assertTrue(store.sharedTable());
      Assert.assertTrue(store.fetchPersistentState());
      Assert.assertTrue(store.async().enabled());

      b = new ConfigurationBuilder();
      b.loaders().addStore(HBaseCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      HBaseCacheStoreConfiguration store2 = (HBaseCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      Assert.assertFalse(store2.autoCreateTable());
      Assert.assertTrue(store2.entryColumnFamily().equals("ECF"));
      Assert.assertEquals("ET", store2.entryTable());
      Assert.assertEquals("EVF", store2.entryValueField());
      Assert.assertEquals("XT", store2.expirationTable());
      Assert.assertEquals("XCF", store2.expirationColumnFamily());
      Assert.assertEquals("XVF", store2.expirationValueField());
      Assert.assertEquals("myhost", store2.hbaseZookeeperQuorumHost());
      Assert.assertEquals(4321, store2.hbaseZookeeperClientPort());
      Assert.assertTrue(store2.sharedTable());
      Assert.assertTrue(store2.fetchPersistentState());
      Assert.assertTrue(store2.async().enabled());
   }
}
