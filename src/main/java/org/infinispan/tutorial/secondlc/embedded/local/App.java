package org.infinispan.tutorial.secondlc.embedded.local;

import static org.infinispan.tutorial.secondlc.util.HibernateUtils.expect;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.expectClusterNodes;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.hit;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.hits;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.miss;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.put;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.puts;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.queryHit;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.queryMiss;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.queryPut;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.unchanged;
import static org.infinispan.tutorial.secondlc.util.HibernateUtils.withCacheExpects;

import java.util.List;
import java.util.function.Function;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import org.infinispan.tutorial.secondlc.model.Event;
import org.infinispan.tutorial.secondlc.model.Person;

/**
 * Application demonstrating Infinispan second-level cache provider for Hibernate.
 *
 * From IDE, run with following VM options to enable assertions and control logging:
 * -ea -Djava.util.logging.config.file=src/main/resources/logging-app.properties
 *
 * By default the cluster should form without passing any extra parameters,
 * but if having trouble cluster to form, add these system properties:
 * -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1
 *
 * To speed up startup cluster member discovery, start with:
 * -Djgroups.join_timeout=1000
 */
public class App {

   static EntityManagerFactory emf1;
   static EntityManagerFactory emf2;

   /**
    * Run through the lifecycle of an entity, verifying second-level cache
    * counts at each step.
    */
   static void jpaMain() throws InterruptedException {
      // Saving (persisting) entities on node 1
      saveEntities("Saving entities on node 1", expect(puts(3)), emf1);

      // Obtaining an entity from cache from node 1 is a hit
      findEntity("Load entity from database on node 1", expect(hit()), emf1);

      // Obtaining an entity from cache from node 2 is a miss and put
      findEntity("Load entity from database on node 2", expect(miss(), put()), emf2);

      // Reloading same entity from node 1 should result in a hit
      findEntity("Reload entity from node 1", expect(hit()), emf1);

      // Reloading same entity from node 2 should result in a hit
      findEntity("Reload entity from node 2", expect(hit()), emf2);

      // Update an entity in node 2, should result in invalidation
      String updatedEventName = updateEntity(1L, "Update entity on node 2", expect(hit(), put()), emf2);

      // Reload updated entity from node 1, should come from database
      findEntity("Reload updated entity from node 1", updatedEventName, expect(miss(), put()), emf1);

      // Evict entity, no changes in cache
      evictEntity("Evict entity in node 1", expect(unchanged()), emf1);

      // Reloading same entity from node 2 should be a miss since evict is cluster-wide
      findEntity("Reload entity from node 2", updatedEventName, expect(miss(), put()), emf2);

      // Reloading same entity from node 1 should be a miss
      findEntity("Reload entity from node 1", updatedEventName, expect(miss(), put()), emf1);

      // Remove cached entity from node 1, which results in a cluster-wide removal
      deleteEntity("Remove cached entity in node 1", expect(hit()), emf1);

      // Trying to load entity from node 2 should not find entity
      findNoEntity("Trying to load entity from node 2", expect(miss()), emf2);

      // Query entities on node 1, expect:
      // * a query cache miss and query cache put
      // * no cache hits since query is miss
      queryEntities("Query entities on node 1", expect(queryMiss(), queryPut()), emf1);

      // Repeat query on node 1, expect:
      // * a query cache hit
      // * two cache hits for the number of entities of the query cache hit
      queryEntities("Repeat query on node 1", expect(queryHit(), hits(2)), emf1);

      // Query entities on node 2, expect:
      // * a query cache miss and query cache put
      // * two cache puts for entities not present for query cache put
      queryEntities("Query entities on node 2", expect(queryMiss(), queryPut(), puts(2)), emf2);

      // Repeat query on node 2, expect:
      // * a query cache hit
      // * two cache hits for the number of entities in cache
      queryEntities("Repeat query on node 2", expect(queryHit(), hits(2)), emf2);

      // Update entity on node 2, should come from cache and update the cache too
      updateEntity(2L, "Update entity on node 2", expect(hit(), put()), emf2);

      // TODO: Statement below randomly fails:
      // get TRACE logs for both situations (needs adding dependencies)
      // and verify why sometimes it passes and sometimes it does not

      // Repeat query on node 2 after update, expect:
      // * a query cache miss and query cache put, because when an entity is updated,
      //   any queries for that type are invalidated
      // * no cache hits or puts since entities are already cached
      queryEntities("Repeat query on node 2 after update", expect(queryMiss(), queryPut()), emf2);

//      // Save cache-expiring entity
//      saveExpiringEntity("Saving expiring entity", expect(puts(1)));
//
//      // Find expiring entity, should come from cache
//      findExpiringEntity("Find expiring entity", expect(hit()));
//
//      // Wait long enough for entity to be expired from cache
//      Thread.sleep(1100);
//
//      // Find expiring entity, after expiration entity should come from DB
//      findExpiringEntity("Find entity after expiration", expect(miss(), put()));
   }

   static void saveEntities(
         String logPrefix, Function<int[], int[]> expects,
         EntityManagerFactory emf) {
      withTxEM(em -> {
         em.persist(new Event("Caught a pokemon!"));
         em.persist(new Event("Hatched an egg"));
         em.persist(new Event("Became a gym leader"));
         return null;
      }, logPrefix, expects, emf);
   }

   static void findEntity(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      findEntity(logPrefix, "Caught a pokemon!", expects, emf);
   }

   static void findEntity(String logPrefix, String eventName,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withEM(em -> {
         Event event = em.find(Event.class, 1L);
         System.out.printf("Found entity: %s%n", event);
         assert event != null;
         assert event.getName().equals(eventName);
         return null;
      }, logPrefix, expects, emf);
   }

   static void findNoEntity(String logPrefix, 
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withEM(em -> {
         Event event = em.find(Event.class, 1L);
         System.out.printf("Found entity: %s%n", event);
         assert event == null;
         return null;
      }, logPrefix, expects, emf);
   }

   static String updateEntity(long id, String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      return withTxEM(em -> {
         Event event = em.find(Event.class, id);
         String newName = "Caught a Snorlax!!";
         event.setName("Caught a Snorlax!!");
         return newName;
      }, logPrefix, expects, emf);
   }

   static void evictEntity(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withEM(em -> {
         em.getEntityManagerFactory().getCache().evict(Event.class, 1L);
         return null;
      }, logPrefix, expects, emf);
   }

   static void deleteEntity(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withTxEM(em -> {
         Event event = em.find(Event.class, 1L);
         em.remove(event);
         return null;
      }, logPrefix, expects, emf);
   }

   static void queryEntities(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withEM(em -> {
         TypedQuery<Event> query = em.createQuery("from Event", Event.class);
         query.setHint("org.hibernate.cacheable", Boolean.TRUE);
         List<Event> events = query.getResultList();
         System.out.printf("Queried events: %s%n", events);
         assert events.size() == 2;
         return null;
      }, logPrefix, expects, emf);
   }

   static void saveExpiringEntity(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withTxEM(em -> {
         em.persist(new Person("Satoshi"));
         return null;
      }, logPrefix, expects, emf);
   }

   static void findExpiringEntity(String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      withEM(em -> {
         Person person = em.find(Person.class, 4L);
         System.out.printf("Found expiring entity: %s%n", person);
         assert person != null;
         assert person.getName().equals("Satoshi");
         return null;
      }, logPrefix, expects, emf);
   }

   static <T> T withEM(
         Function<EntityManager, T> f, String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      EntityManager em = emf.createEntityManager();
      try {
         return withCacheExpects(() -> f.apply(em), logPrefix, expects).apply(em);
      } finally {
         em.close();
      }
   }

   static <T> T withTxEM(
         Function<EntityManager, T> f, String logPrefix,
         Function<int[], int[]> expects, EntityManagerFactory emf) {
      EntityManager em = emf.createEntityManager();
      try {
         // Lambda captures `em` and `f` variables which is not highly recommended,
         // but in this case it makes the code clearer and easier to read.
         return withCacheExpects(() -> {
            EntityTransaction tx = em.getTransaction();
            try {
               tx.begin();
               return f.apply(em);
            } catch (Throwable t) {
               tx.setRollbackOnly();
               throw t;
            } finally {
               if (tx.isActive()) tx.commit();
               else tx.rollback();
            }
         }, logPrefix, expects).apply(em);
      } finally {
         em.close();
      }
   }

   public static void main(String[] args) throws Exception {
      // Obtaining the javax.persistence.EntityManagerFactory
      emf1 = Persistence.createEntityManagerFactory("events");
      emf2 = Persistence.createEntityManagerFactory("events");
      try {
         expectClusterNodes(2);
         jpaMain();
      } finally {
         emf1.close();
         emf2.close();
      }
   }

}
