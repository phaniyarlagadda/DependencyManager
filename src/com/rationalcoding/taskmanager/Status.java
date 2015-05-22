package com.rationalcoding.taskmanager;

/**
 * COMPLETED -- finished executing the task
 * SCHEDULED -- All dependecies satisfied waiting for next thread to process
 * WAITING -- Blocked on dependencies to finish
 * RUNNING -- Currently being processed
 * @author yarlagadda
 *
 */
public enum Status {
   COMPLETED, SCHEDULED, WAITING, RUNNING;

}
