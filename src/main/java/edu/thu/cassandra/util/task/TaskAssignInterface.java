package edu.thu.cassandra.util.task;

import java.util.List;
import java.util.Map;

/**
 * Created by dyf on 15/05/2017.
 */
public interface TaskAssignInterface {
    TaskAssignResult assignTask(int nodeNumber, int taskNumber, Map<Integer, List<Integer>> taskToNode, List<Integer> nodeStatus);
}
