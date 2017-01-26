package org.infinispan.tutorial.secondlc.util;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.hibernate.stat.Statistics;
import org.jboss.logging.Logger;

public class HibernateUtils {

   private static final Logger LOGGER = Logger.getLogger(HibernateUtils.class);

   private static final MBeanServer MBEANS = ManagementFactory.getPlatformMBeanServer();

   private HibernateUtils() {
   }

   @SafeVarargs
   public static Function<int[], int[]> expect(final Function<int[], int[]>... ops) {
      return (counts) -> {
         int[] localCounts = counts.clone();
         for (Function<int[], int[]> op : ops)
            localCounts = op.apply(localCounts);

         return localCounts;
      };
   }

   public static Function<int[], int[]> hit() {
      return hits(1);
   }

   public static Function<int[], int[]> hits(int hits) {
      return (counts) -> updateAt(0, hits, counts);
   }

   public static Function<int[], int[]> miss() {
      return (counts) -> updateAt(1, 1, counts);
   }

   public static Function<int[], int[]> put() {
      return (counts) -> updateAt(2, 1, counts);
   }

   public static Function<int[], int[]> puts(int puts) {
      return (counts) -> updateAt(2, puts, counts);
   }

   public static Function<int[], int[]> queryHit() {
      return (counts) -> updateAt(3, 1, counts);
   }

   public static Function<int[], int[]> queryMiss() {
      return (counts) -> updateAt(4, 1, counts);
   }

   public static Function<int[], int[]> queryPut() {
      return (counts) -> updateAt(5, 1, counts);
   }

   private static int[] updateAt(int i, int n, int[] counts) {
      counts[i] = counts[i] + n;
      return counts;
   }

   public static Function<int[], int[]> unchanged() {
      return Function.identity();
   }

   public static <T> Function<EntityManager, T> withCacheExpects(
         Supplier<T> s, String logPrefix, Function<int[], int[]> expects) {
      return em -> {
         // Calculate cache and query cache counts before operation
         int[] prev = getCacheCounts(em);

         // Apply operation
         T ret = s.get();

         // Calculate cache and query cache counts again
         int[] actual = getCacheCounts(em);

         // Apply expectations
         int[] expected = expects.apply(prev);

         // Log event and counts
         log("%s: %s%n", logPrefix, toStringCounts(actual));

         // Compare cache counts with expected numbers
         assert Arrays.equals(expected, actual) : toStringCounts(actual, expected);

         return ret;
      };
   }

   public static void expectClusterNodes(int numNodes) {
      try {
         ObjectName cacheManagedName = new ObjectName(
               "org.infinispan:type=CacheManager,name=\"SampleCacheManager\",component=CacheManager");

         int size = (int) MBEANS.getAttribute(cacheManagedName, "clusterSize");
         String members = (String) MBEANS.getAttribute(cacheManagedName, "clusterMembers");

         log("Cluster members: %s%n", members);
         assert size == numNodes
               : "Expected " + numNodes + " cluster nodes but got " + size
               + " (members: " + members + ")";
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   public static void log(String format, Object ... args) {
      System.out.printf(format, args);
      LOGGER.infof(format, args);
   }

   private static int[] getCacheCounts(EntityManager em) {
      Statistics statistics = getStatistics(em);
      return new int[]{
            (int) statistics.getSecondLevelCacheHitCount(),
            (int) statistics.getSecondLevelCacheMissCount(),
            (int) statistics.getSecondLevelCachePutCount(),
            (int) statistics.getQueryCacheHitCount(),
            (int) statistics.getQueryCacheMissCount(),
            (int) statistics.getQueryCachePutCount()
            };
   }

   private static String toStringCounts(int[] counts) {
      return String.format(
            "[hits=%d, misses=%d, puts=%d, queryHits=%d, queryMisses=%d, queryPuts=%d]",
            counts[0], counts[1], counts[2], counts[3], counts[4], counts[5]);
   }

   private static String toStringCounts(int[] actual, int[] expected) {
      return String.format(
            "[hits=%d(%d), misses=%d(%d), puts=%d(%d)" +
                  ", queryHits=%d(%d), queryMisses=%d(%d), queryPuts=%d(%d)]",
            actual[0], expected[0], actual[1], expected[1],
            actual[2], expected[2], actual[3], expected[3],
            actual[4], expected[4], actual[5], expected[5]);
   }

   private static Statistics getStatistics(EntityManager em) {
      return ((Session) em.getDelegate()).getSessionFactory().getStatistics();
   }

}
