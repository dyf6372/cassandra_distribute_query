package edu.thu.cassandra.util.task;
import java.util.*;

public class MaxFlow {
    boolean found[];
    int N, cap[][], flow[][], dad[], dist[];

    boolean searchFattest(int source, int sink) {
        Arrays.fill(found, false);
        Arrays.fill(dist, 0);
        dist[source] = Integer.MAX_VALUE / 2;
        while (source != N) {
            int best = N;
            found[source] = true;
            if (source == sink) break;
            for (int k = 0; k < N; k++) {
                if (found[k]) continue;
                int possible = Math.min(cap[source][k] - flow[source][k], dist[source]);
                if (dist[k] < possible) {
                    dist[k] = possible;
                    dad[k] = source;
                }
                if (dist[k] > dist[best]) best = k;
            }
            source = best;
        }
        return found[sink];
    }

    boolean searchShortest(int source, int sink) {
        Arrays.fill(found, false);
        Arrays.fill(dist, Integer.MAX_VALUE/2);
        dist[source] = 0;
        while (source != N) {
            int best = N;
            found[source] = true;
            if (source == sink) break;
            for (int k = 0; k < N; k++) {
                if (found[k]) continue;
                if (cap[source][k] - flow[source][k] > 0) {
                    if (dist[k] > dist[source] + 1){
                        dist[k] = dist[source] + 1;
                        dad[k] = source;
                    }
                }
                if (dist[k] < dist[best]) best = k;
            }
            source = best;
        }
        return found[sink];
    }

    public int getMaxFlow(int cap[][], int source, int sink) {
        this.cap = cap;
        N = cap.length;
        found = new boolean[N];
        flow = new int[N][N];
        dist = new int[N+1];
        dad = new int[N];

        int totflow = 0;
        while (searchFattest(source, sink)) {
            int amt = Integer.MAX_VALUE;
            for (int x = sink; x != source; x = dad[x])
                amt = Math.min(amt, cap[dad[x]][x] - flow[dad[x]][x]);
            for (int x = sink; x != source; x = dad[x]) {
                flow[dad[x]][x] += amt;
                flow[x][dad[x]] -= amt;
            }
            totflow += amt;
        }

        return totflow;
    }
}