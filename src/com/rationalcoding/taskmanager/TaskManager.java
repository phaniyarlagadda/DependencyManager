package com.rationalcoding.taskmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

/**
 * Service class to parallelizes dependency download , job scheduling etc.
 * Uses concepts of topological sort.
 * @author yarlagadda
 *
 * @param <T>
 */
public class TaskManager<T> {

   private Map<T, ArrayList<T>> reverseDependencyGraph;
   private Map<T, Integer> dependencyWeights;
   private BlockingQueue<TaskRunnable<T>> dependencyQueue;
   private Map<T, Status> dependencyStatus;
   // default thread pool size
   private final int DEFAULT_POOL_SIZE = 3;
   private final int ALIVE_TIME = 10;
   private final int MAX_WAIT_TIME = 60;
   
   /**
    * Tasks are ordered respecting the dependencies. A dependency graph has to be passed as input.
    * Tasks are scheduled on multiple threads. Jobs with highest dependencies are prioritized.
    * It is assumed graph does not have cycles and there is atleast one task that does not 
    * @param dependencyGraph
    */
   public void orderAndExecuteTasks(Map<T, ArrayList<T>> dependencyGraph) {
      // reverse dependency graph is not changed once initalized
      reverseDependencyGraph = new HashMap<T, ArrayList<T>>();
      dependencyStatus = new ConcurrentHashMap<T, Status>();
      dependencyQueue = new PriorityBlockingQueue<TaskRunnable<T>>();
      dependencyWeights = new HashMap<T, Integer>();
      
      buildReverseDependencyGraph(dependencyGraph, reverseDependencyGraph);
      System.out.println("Printing reverse dependency order");
      printGraph(reverseDependencyGraph);
      System.out.println("End printing reverse dependency order");
      System.out.println("Start printing dependecy weight map");
      for(T dependency: dependencyWeights.keySet()){
         System.out.println("For dependency :"+dependency+" weight is "+dependencyWeights.get(dependency));
      }
      System.out.println("End printing dependecy weight map");
      System.out.println("Starting dependencies download");
      long startTime = System.currentTimeMillis();
      pollAndRunTasks();
      long endTime = System.currentTimeMillis();
      System.out.println("Tasks finished. Time to execute : " + ((endTime - startTime) / 1000) + " seconds");
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   private void pollAndRunTasks() {
      // initalize the thread pool and submit first set of independent tasks
      TaskManagerThreadPoolExecutor taskExecutor = new TaskManagerThreadPoolExecutor(reverseDependencyGraph,
            dependencyWeights, dependencyStatus, DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE, ALIVE_TIME,
            TimeUnit.SECONDS, dependencyQueue);
      taskExecutor.prestartAllCoreThreads();
      // wait for pool to finish up all tasks
      try {
         taskExecutor.awaitTermination(MAX_WAIT_TIME, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
         System.out.println("Thread pool did not shutdown properly");
         e.printStackTrace();
      }
      taskExecutor.printTaskOrder();
   }

   private void buildReverseDependencyGraph(Map<T, ArrayList<T>> dependencyGraph,
         Map<T, ArrayList<T>> reverseDependencyGraph) {
      for (T node : dependencyGraph.keySet()) {
         ArrayList<T> dependencies = dependencyGraph.get(node);
         for (T d : dependencies) {
            if (reverseDependencyGraph.containsKey(d)) {
               if (!reverseDependencyGraph.get(d).contains(node)) {
                  reverseDependencyGraph.get(d).add(node);
               }
            } else {
               reverseDependencyGraph.put(d, new ArrayList<T>());
               reverseDependencyGraph.get(d).add(node);
            }
         }
         dependencyStatus.put(node, Status.WAITING);
      }
      for (T dependecy : dependencyGraph.keySet()) {
         if (dependencyGraph.get(dependecy).size() == 0) {
            int edgeWeight = 0;
            if (reverseDependencyGraph.containsKey(dependecy)) {
               edgeWeight = reverseDependencyGraph.get(dependecy).size();
            } 
            TaskRunnable<T> obj = new TaskRunnable<T>(dependecy, edgeWeight);
            dependencyQueue.add(obj);
         } else {
            dependencyWeights.put(dependecy, dependencyGraph.get(dependecy).size());
         }
      }
   }
   
   private void printGraph(Map<T, ArrayList<T>> dependencyGraph) {
      for (T s : dependencyGraph.keySet()) {
         System.out.println(s + "->" + StringUtils.join(dependencyGraph.get(s), ","));
      }
      for (T s : dependencyWeights.keySet()) {
         System.out.println(s + "->" + dependencyWeights.get(s));
      }
   }

}
