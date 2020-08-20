package com.alibaba.alink.Linprog.util;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorIterator;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.Arrays;
import java.util.List;

public class LinProUtils {
    static public double[][] getBlindStart(int m, int n) {
        double[][] result = new double[5][];
        result[0] = new double[n];
        result[1] = new double[m];
        result[2] = new double[n];
        result[3] = new double[1];
        result[4] = new double[1];
        Arrays.fill(result[0], 1.0);
        Arrays.fill(result[1], 0.0);
        Arrays.fill(result[2], 1.0);
        Arrays.fill(result[3], 1.0);
        Arrays.fill(result[4], 1.0);
        return result;
    }

    static public int[] getStartTail(int m, int taskId, int taskNum){
        int[] range = new int[2];
        int partSize    =   m / taskNum;
        int resting     =   m % taskNum;
        if(taskId < resting){
            range[0] = (partSize+1) * taskId;
            range[1] = (partSize+1) * (taskId+1);
        }else{
            range[1] = m - partSize * (taskNum - taskId - 1);
            range[0] = m - partSize * (taskNum - taskId);
        }
        return range;
    }

    static public double[][] indicators(DenseMatrix A, double[] b, double[] c,
                                      double[] x, double[] y, double[] z, double tau, double kappa,
                                      int taskId, int taskNum) {
        int m = A.numRows();
        int n = A.numCols();
        int[] rangeM = getStartTail(m, taskId, taskNum);
        int[] rangeN = getStartTail(n, taskId, taskNum);
        double [][] blindStart = getBlindStart(m, n);
        double [] x0 = blindStart[0];
        double [] y0 = blindStart[1];
        double [] z0 = blindStart[2];
        double tau0  = blindStart[3][0];
        double kappa0= blindStart[4][0];

        double [][] result = new double[6][];
        result[0]   = r_p(A, b, x0,     tau0, rangeM[0], rangeM[1]);
        result[1]   = r_d(A, c, y0, z0, tau0, rangeN[0], rangeN[1]);
        result[2]   = new double[1];
        result[2][0]= r_g(b, c, x0, y0, kappa0);
        result[3]   = r_p(A, b, x,     tau, rangeM[0], rangeM[1]);
        result[4]   = r_d(A, c, y,  z, tau, rangeN[0], rangeN[1]);
        result[5]   = new double[1];
        result[5][0]= r_g(b, c, x, y, kappa0);
        return result;
    }

    static private double[] r_p(DenseMatrix A, double[] b,
                              double[] x, double tau, int s, int t){
        int m = A.numRows();
        int n = A.numCols();
        DenseVector vector_b = new DenseVector(b);
        DenseVector vector_x = new DenseVector(x);
        double A_dot_x = 0;
        for(int i=s; i<t; i++) {
            DenseVector a = new DenseVector(A.getRow(i));
            A_dot_x -= a.dot(vector_x);
        }
        vector_b.scale(tau*(t-s)/m).plusScaleEqual(new DenseVector(n), A_dot_x);
        return  vector_b.getData();
    }

    static private double[] r_d(DenseMatrix A, double[] c, double[] y, double[] z,
                                double tau, int s, int t){
        int n = A.numCols();
        int m = A.numRows();
        DenseVector vector_c = new DenseVector(c);
        DenseVector vector_y = new DenseVector(y);
        double A_T_dot_y = 0;
        for(int i=s; i<t; i++) {
            DenseVector a = new DenseVector(A.getColumn(i));
            A_T_dot_y -= a.dot(vector_y);
        }
        vector_c.scale(tau*(t-s)/n).plusScaleEqual(new DenseVector(m), A_T_dot_y);
        vector_c.minusEqual(new DenseVector(z));
        return  vector_c.getData();
    }

    static private double r_g(double[] b, double[] c, double[] x, double[] y, double kappa){
        DenseVector vector_b = new DenseVector(b);
        DenseVector vector_c = new DenseVector(c);
        DenseVector vector_x = new DenseVector(x);
        DenseVector vector_y = new DenseVector(y);
        kappa += vector_c.dot(vector_x);
        kappa -= vector_b.dot(vector_y);
        return kappa;
    }

    static private double mu(double[] x, double[] z, double tau, double kappa){
        DenseVector vector_x = new DenseVector(x);
        DenseVector vector_z = new DenseVector(z);
        double result = tau * kappa / (vector_x.size()+1) + vector_x.dot(vector_z);
        return result;
    }

    static public double norm(double[] a){
        DenseVector vector_a = new DenseVector(a);
        return vector_a.normL2();
    }

    static public DenseVector Obj2DenseVector(String name, ComContext context, boolean hasHeader){
        double[] obj=   context.getObj(name);
        int n   =   obj.length;
        if(hasHeader){
            double[]v   =   new double[n-2];
            System.arraycopy(obj, 2, v, 0, n-2);
            return new DenseVector(v);
        }else{
            return new DenseVector(obj);
        }
    }

    static public void putDenseVector(DenseVector v, String name, ComContext context){
        double[] d = new double[v.size()+2];
        System.arraycopy(v.getData(), 0, d, 2, v.size());
        d[0]    =   0;
        d[1]    =   0;
        context.putObj(name, d);
    }

    static public DenseVector VectorTimes(DenseVector v1, DenseVector v2){
        int n   =   v1.size();
        DenseVector v = new DenseVector(n);
        for(int i = 0; i < n ; i++)
            v.set(i, v1.get(i)*v2.get(i));
        return v;
    }

    static public DenseVector VectorDivs(DenseVector v1, DenseVector v2){
        int n   =   v1.size();
        DenseVector v = new DenseVector(n);
        for(int i = 0; i < n ; i++)
            v.set(i, v1.get(i)/v2.get(i));
        return v;
    }
    static public DenseVector VectorPlusConst(DenseVector v1, double c){
        int n   =   v1.size();
        DenseVector v = new DenseVector(n);
        for(int i = 0; i < n ; i++)
            v.set(i, v1.get(i)+c);
        return v;
    }

    static public double getStep(DenseVector x, DenseVector d_x,
                                 DenseVector z, DenseVector d_z,
                                 double tau, double d_tau,
                                 double kappa, double d_kappa,
                                 double alpha0){
        double alpha_x = minAlpha(x, d_x, alpha0);
        double alpha_z = minAlpha(z, d_z, alpha0);
        double alpha_tau = d_tau<0 ? alpha0 * tau / -d_tau : 1.0;
        double alpha_kappa = d_kappa<0 ? alpha0 * kappa / -d_kappa : 1.0;
        double[] result = {alpha_x, alpha_z, alpha_tau,alpha_kappa};
        double min_rst = 1.0;
        for(int i = 0 ; i < 4 ; i++)
                min_rst = Math.min(result[i], min_rst);
        return min_rst;
    }

    static private double minAlpha(DenseVector v1, DenseVector v2, double alpha0){
        boolean flag = false;
        double min_res = Double.MAX_VALUE;
        for(int i = 0; i<v1.size();i++){
            if(v2.get(i)<0 && v1.get(i)/(-v2.get(i))<min_res) {
                min_res = v1.get(i) / (-v2.get(i));
                flag = true;
            }
        }
        if(flag)
            return alpha0 * min_res;
        else
            return 1.0;
    }

    static public Tuple5<DenseVector,DenseVector,DenseVector,Double,Double> doStep(
            DenseVector x, DenseVector d_x, DenseVector y, DenseVector d_y,
            DenseVector z, DenseVector d_z,
            double tau, double d_tau, double kappa, double d_kappa,
            double alpha)
    {
        Tuple5<DenseVector,DenseVector,DenseVector,Double,Double> result = new Tuple5<>();
        result.setField(x.plus(d_x.scale(alpha)), 0);
        result.setField(y.plus(d_y.scale(alpha)), 1);
        result.setField(z.plus(d_z.scale(alpha)), 2);
        result.setField(tau + alpha * d_tau, 3);
        result.setField(kappa + alpha * d_kappa, 4);
        return result;
    }

    static public double[] Vector2List (DenseVector v){
        double[] result = new double[v.size()+2];
        result[0] = 0;
        result[1] = 0;
        System.arraycopy(v.getData(), 0, result, 2, v.size());
        return result;
    }
}

