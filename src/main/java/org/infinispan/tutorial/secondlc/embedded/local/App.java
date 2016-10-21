package org.infinispan.tutorial.secondlc.embedded.local;

import static org.infinispan.tutorial.secondlc.util.HibernateUtils.expect;
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
 */
public class App {

   static EntityManagerFactory emf;

   /**
    * Run through the lifecycle of an entity, verifying second-level cache
    * counts at each step.
    */
   static void jpaMain() throws InterruptedException {
      // Saving (persisting) entities
      saveEntities("Saving entities", expect(puts(3)));

      // Obtaining an entity from cache
      findEntity("Load entity, should come from cache", expect(hit()));

      // Reloading same entity should come from cache
      findEntity("Reload entity", expect(hit()));

      // Update an entity in cache
      String updatedEventName = updateEntity(1L, "Update entity", expect(hit(), put()));

      // Reload updated entity, should come from cache
      findEntity("Reload updated entity", updatedEventName, expect(hit()));

      // Evict entity from cache, no changes in cache
      evictEntity("Evict entity", expect(unchanged()));

      // Reload evicted entity, should come from DB
      findEntity("Reload evicted entity", updatedEventName, expect(miss(), put()));

      // Remove cached entity
      deleteEntity("Remove cached entity", expect(hit()));

      // Query entities, expect:
      // * no cache hits since query is not cached
      // * a query cache miss and query cache put
      queryEntities("Query entities", expect(queryMiss(), queryPut()));

      // Repeat query, expect:
      // * two cache hits for the number of entities in cache
      // * a query cache hit
      queryEntities("Repeat query", expect(hits(2), queryHit()));

      // Update entity, should come from cache and update the cache too
      updateEntity(2L, "Update entity", expect(hit(), put()));

      // Repeat query after update, expect:
      // * no cache hits or puts since entities are already cached
      // * a query cache miss and query cache put, because when an entity is updated,
      //   any queries for that type are invalidated
      queryEntities("Repeat query after update", expect(queryMiss(), queryPut()));

      // Save cache-expiring entity
      saveExpiringEntity("Saving expiring entity", expect(puts(1)));

      // Find expiring entity, should come from cache
      findExpiringEntity("Find expiring entity", expect(hit()));

      // Wait long enough for entity to be expired from cache
      Thread.sleep(1100);

      // Find expiring entity, after expiration entity should come from DB
      findExpiringEntity("Find entity after expiration", expect(miss(), put()));
   }

   static void saveEntities(String logPrefix, Function<int[], int[]> expects) {
      withTxEM(em -> {
         em.persist(new Event("Caught a pokemon!"));
         em.persist(new Event("Hatched an egg"));
         em.persist(new Event("Became a gym leader"));
         return null;
      }, logPrefix, expects);
   }

   static void findEntity(String logPrefix, Function<int[], int[]> expects) {
      findEntity(logPrefix, "Caught a pokemon!", expects);
   }

   static void findEntity(String logPrefix, String eventName, Function<int[], int[]> expects) {
      withEM(em -> {
         Event event = em.find(Event.class, 1L);
         System.out.printf("Found entity: %s%n", event);
         assert event != null;
         assert event.getName().equals(eventName);
         return null;
      }, logPrefix, expects);
   }

   static String updateEntity(long id, String logPrefix, Function<int[], int[]> expects) {
      return withTxEM(em -> {
         Event event = em.find(Event.class, id);
         String newName = "Caught a Snorlax!!";
         event.setName("Caught a Snorlax!!");
         return newName;
      }, logPrefix, expects);
   }

   static void evictEntity(String logPrefix, Function<int[], int[]> expects) {
      withEM(em -> {
         em.getEntityManagerFactory().getCache().evict(Event.class, 1L);
         return null;
      }, logPrefix, expects);
   }

   static void deleteEntity(String logPrefix, Function<int[], int[]> expects) {
      withTxEM(em -> {
         Event event = em.find(Event.class, 1L);
         em.remove(event);
         return null;
      }, logPrefix, expects);
   }

   static void queryEntities(String logPrefix, Function<int[], int[]> expects) {
      withEM(em -> {
         TypedQuery<Event> query = em.createQuery("from Event", Event.class);
         query.setHint("org.hibernate.cacheable", Boolean.TRUE);
         List<Event> events = query.getResultList();
         System.out.printf("Queried events: %s%n", events);
         assert events.size() == 2;
         return null;
      }, logPrefix, expects);
   }

   static void saveExpiringEntity(String logPrefix, Function<int[], int[]> expects) {
      withTxEM(em -> {
         em.persist(new Person("Satoshi"));
         return null;
      }, logPrefix, expects);
   }

   static void findExpiringEntity(String logPrefix, Function<int[], int[]> expects) {
      withEM(em -> {
         Person person = em.find(Person.class, 4L);
         System.out.printf("Found expiring entity: %s%n", person);
         assert person != null;
         assert person.getName().equals("Satoshi");
         return null;
      }, logPrefix, expects);
   }

   static <T> T withEM(
         Function<EntityManager, T> f, String logPrefix,
         Function<int[], int[]> expects) {
      EntityManager em = emf.createEntityManager();
      try {
         return withCacheExpects(() -> f.apply(em), logPrefix, expects).apply(em);
      } finally {
         em.close();
      }
   }

   static <T> T withTxEM(
         Function<EntityManager, T> f, String logPrefix,
         Function<int[], int[]> expects) {
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
      emf = Persistence.createEntityManagerFactory("events");
      try {
         jpaMain();
      } finally {
         emf.close();
      }
   }

}
