package aqp;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CountMinSketch {
    private int m;
    private int d;
    private int n;
    private int T;
    private List<int[]> tables;

    public CountMinSketch(int m, int d) {
        if (m <= 0 || d <= 0) {
            throw new IllegalArgumentException("Table size (m) and number of hash functions (d) must be positive");
        }
        this.m = m;
        this.d = d;
        this.n = 0;
        this.T = (int) Math.pow(2, 32);
        this.tables = new ArrayList<>();
        /*
         *       0   1   2   3   . . . m-1
         * h_0   0   0   0   0         0
         * h_1   0   0   0   0         0
         * h_2   0   0   0   0         0
         * h_3   0   0   0   0         0
         * .
         * .
         * h_d-1 0   0   0   0         0
         */
        for (int i = 0; i < d; i++) {
            int[] table = new int[m];
            Arrays.fill(table, 0);
            this.tables.add(table);
        }
    }

    // given an object, generates a hash value from 0 to m which is used as an index by 'add'
    // the table value at this index is then incremented
    private int[] hash(Object x) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(Integer.toString(x.hashCode()).getBytes());
        int[] hashes = new int[d];
        for (int i = 0; i < d; i++) {
            md.update(Integer.toString(i).getBytes());
            byte[] digest = md.digest();
            hashes[i] = Math.abs(Arrays.hashCode(digest) % m);
        }
        return hashes;
    }

    // for count_min_sum value = value of tuple
    public void add(Object x, double value) throws NoSuchAlgorithmException {
        n += value;
        int[] hashes = hash(x);
        for (int i = 0; i < d; i++) {
            int index = hashes[i];
            tables.get(i)[index] += value;
            tables.get(i)[index] = Math.min(tables.get(i)[index], T);
        }
    }

    // for count_min value = 1
    public void add(Object x) throws NoSuchAlgorithmException {
        add(x, 1);
    }

    public int query(Object x) throws NoSuchAlgorithmException {
        int[] hashes = hash(x);
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < d; i++) {
            int index = hashes[i];
            min = Math.min(min, tables.get(i)[index]);
        }
        return min;
    }

    public void reset(Object x) throws NoSuchAlgorithmException {
        int[] hashes = hash(x);
        for (int i = 0; i < d; i++) {
            int index = hashes[i];
            tables.get(i)[index] = 0;
        }
    }

    public int getN() {
        return n;
    }

    public double density() {
        int ans = 0;
        for (int[] arr : tables) {
            for (int i = 0; i < m; i++) {
                if (arr[i] == 0) {
                    ans++;
                }
            }
        }
        return 1 - (double) ans / (m * d);
    }
}
