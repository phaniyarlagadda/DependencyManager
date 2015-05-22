package com.rationalcoding.taskmanager.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.rationalcoding.taskmanager.TaskManager;

/**
 * Basic test to verify task manager.
 * TODO : add more tests and assert the output
 * @author yarlagadda
 *
 */
public class TaskManagerTest {
   
   @Test
   public void basicTest() throws IOException{
      TaskManager<String> manager = new TaskManager<String>();
      File inputFile = new File("sample1.txt");
      Map<String, ArrayList<String>> dependencyGraph = parseInput(inputFile);
      printGraph(dependencyGraph);
      manager.orderAndExecuteTasks(dependencyGraph);
   }
   
   @Test
   public void basicTest2() throws IOException{
      TaskManager<String> manager = new TaskManager<String>();
      File inputFile = new File("sample2.txt");
      Map<String, ArrayList<String>> dependencyGraph = parseInput(inputFile);
      printGraph(dependencyGraph);
      manager.orderAndExecuteTasks(dependencyGraph);
   }
   
   private void printGraph(Map<String, ArrayList<String>> dependencyGraph){
      for(String s:dependencyGraph.keySet()){
         System.out.println(s+"->"+StringUtils.join(dependencyGraph.get(s), ","));
      }
   }
   
   private Map<String, ArrayList<String>> parseInput(File inputFile) throws IOException{
      Map<String, ArrayList<String>> dependencyGraph = new HashMap<String, ArrayList<String>>();
      List<String> inputLines = FileUtils.readLines(inputFile);
      for(String line: inputLines){
         if(!line.contains("->")){
            dependencyGraph.put(line, new ArrayList<String>());
         }else{
            String[] tokens = line.split("->");
            if(!dependencyGraph.containsKey(tokens[0].trim())){
               dependencyGraph.put(tokens[0].trim(), new ArrayList<String>());
            }
            String[] dependencies = tokens[1].trim().split(",");
            for(String d : dependencies)
               dependencyGraph.get(tokens[0].trim()).add(d.trim());
         }
      }
      return dependencyGraph;
   }

}
