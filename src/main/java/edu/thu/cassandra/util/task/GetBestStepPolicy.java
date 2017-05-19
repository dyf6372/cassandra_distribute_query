package edu.thu.cassandra.util.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dyf on 15/05/2017.
 */
public class GetBestStepPolicy implements TaskAssignInterface{

    @Override
    public TaskAssignResult assignTask(int nodeNumber, int taskNumber, Map<Integer, List<Integer>> taskToNode, List<Integer> nodeStatus) {
        TaskAssignResult taskAssignResult = new TaskAssignResult();

        Map<Integer, Integer> assign = new HashMap<>();
        int[] cost = new int[nodeNumber];
        for(Map.Entry<Integer, List<Integer>> entry:taskToNode.entrySet()){
            int node = entry.getValue().get(0);
            int nodeCost = nodeStatus.get(node - 1);
            for(Integer nodeEntry: entry.getValue()){
                if(nodeStatus.get(nodeEntry - 1) < nodeCost){
                    node = nodeEntry;
                    nodeCost = nodeStatus.get(nodeEntry - 1);
                }
            }
            assign.put(entry.getKey(), node);
            cost[node - 1] += 1 * nodeStatus.get(node - 1);
        }

        int maxCost = 0;
        for(int i = 0; i< nodeNumber; i++)
            maxCost = maxCost > cost[i] ? maxCost : cost[i];
        taskAssignResult.cost = maxCost;
        taskAssignResult.assign = assign;
        return taskAssignResult;
    }

}
