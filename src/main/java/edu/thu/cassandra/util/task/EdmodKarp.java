package edu.thu.cassandra.util.task;

import java.util.LinkedList;
import java.util.Queue;

public class EdmodKarp {
    int maxdata = Integer.MAX_VALUE;
    int[][] capacity;
    int[] flow;
    int[] pre;
    int n;
    Queue<Integer> queue;

    public EdmodKarp(int[][] capacity) {
        this.capacity = capacity;
        this.n = capacity.length;
        this.pre = new int[n];
    }

    //广度优先遍历的查找一条src到des的路径
    int BFS(int src, int des) {
        int i;
        this.queue = new LinkedList<Integer>();
        this.flow = new int[n];
        for (i = 0; i < n; ++i) {
            pre[i] = -1;
        }
        pre[src] = -2;
        flow[src] = maxdata;
        queue.add(src);
        while (!queue.isEmpty()) {
            int index = queue.poll();
            if (index == des)            //找到了增广路径
                break;
            for (i = 0; i < n; ++i) {
                //找到非源节点未被访问过的可达结点，计算其流量
                if (i != src && capacity[index][i] > 0 && pre[i] == -1) {
                    pre[i] = index; //记录前驱
                    flow[i] = Math.min(capacity[index][i], flow[index]);   //关键：迭代的找到增量
                    queue.add(i);
                }
            }
        }
        if (pre[des] == -1)      //残留图中不再存在增广路径
            return -1;
        else
            return flow[des];

    }

    int maxFlow(int src, int des) {
        int increasement = 0;
        int sumflow = 0;
        while ((increasement = BFS(src, des)) != -1) {

            int k = des;          //利用前驱寻找路径
            while (k != src) {
                int last = pre[k];
                capacity[last][k] -= increasement; //改变正向边的容量
                capacity[k][last] += increasement; //改变反向边的容量
                k = last;
            }

            /*
            System.out.println("-------改变后---------");
            for (int j = 0; j < n; j++) {
                for (int x = 0; x < n; x++) {
                    System.out.print("---" + capacity[j][x]);
                }
                System.out.println();
            }
            */
            sumflow += increasement;
        }
        return sumflow;
    }

    public static void main2() {
        int[][] matrix = new int[100][100];
        matrix[0][1] = 4;
        matrix[0][3] = 2;
        matrix[1][2] = 3;
        matrix[1][3] = 2;
        matrix[2][3] = 1;

        for(int i = 0; i<50; i++){
            matrix[i][i+1] = 1;
            matrix[i][i+2] = 3;
            matrix[i][i+3] = 4;
            matrix[i][i+4] = 2;
            matrix[i][i+5] = 3;
            matrix[i][i+6] = 1;
            matrix[i][i+7] = 5;
        }

        EdmodKarp edm = new EdmodKarp(matrix);

        System.out.println("-------初始化---------");
        for (int j = 0; j < edm.n; j++) {
            for (int k = 0; k < edm.n; k++) {
                System.out.print("---" + edm.capacity[j][k]);
            }
            System.out.println();
        }
        int actual = edm.maxFlow(0, 50);
        int expected = 5;
        System.out.println("-------最终结果---------");
        for (int j = 0; j < edm.n; j++) {
            for (int k = 0; k < edm.n; k++) {
                System.out.print("---" + edm.capacity[j][k]);
            }
            System.out.println();
        }
        System.out.println(actual);
        //Assert.assertEquals(expected, actual);
    }
}