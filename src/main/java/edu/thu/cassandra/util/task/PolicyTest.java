package edu.thu.cassandra.util.task;

import java.util.*;

/**
 * Created by dyf on 15/05/2017.
 */
public class PolicyTest {
    public static void main(String[] args){
        int nodeNumber = 3;
        int taskNumber = 30;
        Map<Integer, List<Integer>> taskToNode = new HashMap<>();

        Random random = new Random();

        for(int i = 1; i <= taskNumber; i++){
            List<Integer> list = new ArrayList<>();
            for(int j = 1; j <= 3; j++){
                //list.add(random.nextInt(nodeNumber) + 1);
                list.add(j);
            }
            taskToNode.put(i, list);
        }

        List<Integer> nodeStatus = new ArrayList<>();
        for(int i = 0; i< nodeNumber; i++){
            //nodeStatus.add(random.nextInt(3) + 1);
            nodeStatus.add(1);
        }

        System.out.println(taskToNode);
        System.out.println(nodeStatus);
        TaskAssignInterface taskAssignInterface = new GetBestStepPolicy();
        TaskAssignResult taskAssignResult = taskAssignInterface.assignTask(nodeNumber, taskNumber, taskToNode, nodeStatus);
        System.out.println(taskAssignResult.cost);
        System.out.println(taskAssignResult.assign);


        TaskAssignInterface taskAssignInterface2 = new GetMaxFlowPolicy();
        TaskAssignResult taskAssignResult2 = taskAssignInterface2.assignTask(nodeNumber, taskNumber, taskToNode, nodeStatus);
        System.out.println(taskAssignResult2.cost);
        System.out.println(taskAssignResult2.assign);
    }
}
