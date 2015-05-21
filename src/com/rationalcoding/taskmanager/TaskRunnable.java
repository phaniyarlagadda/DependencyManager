package com.rationalcoding.taskmanager;

import java.util.Random;

/**
 * Runnable for executing the task
 * @author yarlagadda
 *
 * @param <T>
 */
public class TaskRunnable<T> implements Runnable, Comparable<TaskRunnable>{
   
   private T dependency;
   private int edgeWeight;
   
   public TaskRunnable(T dependency, int edgeWeight) {
      this.dependency = dependency;
      this.edgeWeight = edgeWeight;
   }

   @Override
   public void run()  {
      System.out.println("Starting to execute task : " + dependency+". Edge weight :"+edgeWeight);
      int waitTime = 0;
      try {
         Random rnd = new Random();
         waitTime = rnd.nextInt(10);
         Thread.sleep(waitTime*1000);
      } catch (InterruptedException e) {
         System.out.println("Thread interrupted");
         e.printStackTrace();
      }
      System.out.println("Finished executing task : " + dependency+" "+waitTime+" seconds");
   }
   
   public T getDependency() {
      return dependency;
   }

   public void setDependency(T dependency) {
      this.dependency = dependency;
   }

   @Override
   public int compareTo(TaskRunnable o) {
      // reverse the order since we are looking for vertex with maximum number of edges
      return edgeWeight > o.getEdgeWeight() ? -1:edgeWeight == o.getEdgeWeight()?0:1;
   }

   public int getEdgeWeight() {
      return edgeWeight;
   }

   public void setEdgeWeight(int weight) {
      this.edgeWeight = weight;
   }

}
