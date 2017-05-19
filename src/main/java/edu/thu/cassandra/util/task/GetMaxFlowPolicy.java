package edu.thu.cassandra.util.task;

import javax.validation.constraints.Max;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dyf on 15/05/2017.
 */
public class GetMaxFlowPolicy implements TaskAssignInterface{

    @Override
    public TaskAssignResult assignTask(int nodeNumber, int taskNumber, Map<Integer, List<Integer>> taskToNode, List<Integer> nodeStatus) {
        return getMyMaxFlow(nodeNumber, taskNumber, taskToNode, nodeStatus);
    }

    public TaskAssignResult getMyMaxFlow(int nodeNumber, int taskNumber, Map<Integer, List<Integer>> taskToNode, List<Integer> nodeStatus) {
        int left = 0;
        int right = 10 * taskNumber;

        MaxFlow flow = new MaxFlow();
        int[][] matrix = null;

        while(true){
            if (right <= left){
                break;
            }
            int mid = (int)(left + (right-left)/2);

            matrix = getMyMaxFlow(nodeNumber, taskNumber, taskToNode, nodeStatus, mid);

            int maxFlow = flow.getMaxFlow(matrix, 0, nodeNumber + taskNumber + 1);
            if (maxFlow < taskNumber){
                left = mid + 1;
            }else {
                right = mid;
            }
        }

        matrix = getMyMaxFlow(nodeNumber, taskNumber, taskToNode, nodeStatus, right);
        flow.getMaxFlow(matrix, 0, nodeNumber + taskNumber + 1);

        TaskAssignResult taskAssignResult = new TaskAssignResult();
        Map<Integer, Integer> assign = new HashMap<>();
        for(int i = 1; i <= taskNumber; i++){
            for(int j = taskNumber + 1; j <= taskNumber + nodeNumber; j++){
                if(flow.flow[i][j] == 1){
                    assign.put(i, j - taskNumber);
                }
            }
        }
        taskAssignResult.cost = right;
        taskAssignResult.assign = assign;
        return taskAssignResult;
    }

    public int[][] getMyMaxFlow(int nodeNumber, int taskNumber, Map<Integer, List<Integer>> taskToNode, List<Integer> nodeStatus, int T){
        int allNodeNumber = nodeNumber + taskNumber + 2;
        int[][] matrix = new int[allNodeNumber][allNodeNumber];
        for(int i = 0; i<= taskNumber; i++){
            matrix[0][i] = 1;
            //matrix[i][0] = 1;
        }

        for(Map.Entry<Integer, List<Integer>> entry: taskToNode.entrySet()){
            int realStartNode = entry.getKey();
            for(int node: entry.getValue()) {
                int realEndNode = node + taskNumber;
                matrix[realStartNode][realEndNode] = 1;
                //matrix[realEndNode][realStartNode] = 1;
            }
        }

        for(int i = 0; i < nodeNumber; i++){
            matrix[taskNumber + i + 1][allNodeNumber - 1] = (int)(T / nodeStatus.get(i));
            //matrix[allNodeNumber - 1][taskNumber + i + 1] = (int)(T / nodeStatus.get(i));
        }

        return matrix;
    }

}
