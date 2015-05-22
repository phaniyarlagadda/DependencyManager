package com.rationalcoding.taskmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;

/**
 * Thread pool executor for running tasks. Manages the dependencies and updating the dependency count.
 * Updates status of tasks after completion and before starting
 * @author yarlagadda
 *
 * @param <T>
 */
public class TaskManagerThreadPoolExecutor<T> extends ThreadPoolExecutor{
   
   private Map<T, ArrayList<T>> reverseDependencyGraph;
   private Map<T, Integer> dependencyWeights;
   private Map<T, Status> dependencyStatus;
   private Lock reeentrantLock;
   private List<T> taskOrderList;
   
   public TaskManagerThreadPoolExecutor(Map<T, ArrayList<T>> reverseDependencyGraph, Map<T, Integer> dependencyWeights,
         Map<T, Status> dependencyStatus,
         int corePoolSize, int maximumPoolSize, long keepAliveTime,
         TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
      this.dependencyStatus = dependencyStatus;
      this.reverseDependencyGraph = reverseDependencyGraph;
      this.dependencyWeights = dependencyWeights;
      reeentrantLock = new ReentrantLock();
      taskOrderList = new ArrayList<T>();
   }

   public TaskManagerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
         TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);      
   }
   
   @SuppressWarnings("unchecked")
   @Override
   protected void afterExecute(Runnable r, Throwable t){
      // update status and reduce the dependency weight
      if(r instanceof TaskRunnable<?>){
         TaskRunnable<?> taskRunnable = (TaskRunnable<?> ) r;
         dependencyStatus.put((T)taskRunnable.getDependency(), Status.COMPLETED);
         updateDependencyCount((T ) taskRunnable.getDependency());
      }else{
         // TODO :handle exception
      }
   }
   
   @SuppressWarnings("unchecked")
   @Override
   protected void beforeExecute(Thread t, Runnable r){
      // update status to executing
      if(r instanceof TaskRunnable<?>){
         TaskRunnable<?> taskRunnable = (TaskRunnable<?> ) r;
         dependencyStatus.put((T)taskRunnable.getDependency(), Status.RUNNING);
      }else{
         // TODO :handle exception
      }
   }
   
   private void updateDependencyCount(T dependency) {
      // this area is gated to avoid multiple threads changing data at same time which causes inconsitencies
      reeentrantLock.lock();
      try{
         taskOrderList.add(dependency);
         // if other tasks depend on this task update their count
         // if no tasks depend on this then continue
         if (reverseDependencyGraph.containsKey(dependency)) {
            for (T d : reverseDependencyGraph.get(dependency)) {
               int remainingDependencyCount = dependencyWeights.get(d);
               remainingDependencyCount--;
               if (remainingDependencyCount == 0) {
                  // if all dependencies have been satisfied for this task
                  // add it to the queue for processing
                  int edgeWeight = 0;
                  if (reverseDependencyGraph.containsKey(d)) {
                     edgeWeight = reverseDependencyGraph.get(d).size();
                  } 
                  TaskRunnable<T> obj = new TaskRunnable<T>(d, edgeWeight);
                  getQueue().add(obj);
                  // remove the node from dependency weight map
                  dependencyWeights.remove(d);
                  // update status of the dependency
                  dependencyStatus.put(d, Status.SCHEDULED);
               } else {
                  dependencyWeights.put(d, remainingDependencyCount);
               }
            }
         }
         // if all tasks are finished shutdown the pool
         if(dependencyWeights.size() == 0 && getQueue().size() == 0){
            shutdown();
         }
         printStatus();
      }finally{
         reeentrantLock.unlock();
      }
   }
   
   /**
    * Util method to print the status of tasks
    */
   private void printStatus() {
      System.out.println("==============Start Printing Status==================");
      for (T d : dependencyStatus.keySet()) {
         System.out.println(d + "->" + dependencyStatus.get(d));
      }
      System.out.println("==============End Printing Status====================");
   }
   
   public void printTaskOrder(){
      System.out.println("Order of task completion : "+StringUtils.join(taskOrderList, "->"));
   }
   

}
