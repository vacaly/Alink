package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowCollector;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.Linprog.LPInnerPointBatchOp.VECTOR;
import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.MIN_VALUE;

public class MatMulVecComplete extends CompleteResultFunction {
    @Override
    public List<Row> calc(ComContext context) {
        DenseVector x_hat = context.getObj(LPInnerPointBatchOp.LOCAL_X_HAT);
        double[] c  = context.getObj(LPInnerPointBatchOp.STATIC_C);
        double c0   = context.getObj(LPInnerPointBatchOp.STATIC_C0);
        int n   =   context.getObj(LPInnerPointBatchOp.N);
        double[][] bound = new double[3][n];
        Arrays.fill(bound[0],0.0);
        Arrays.fill(bound[1],MAX_VALUE);//upper bound
        Arrays.fill(bound[2],MIN_VALUE);//lower bound
        List<Row> upperBoundsListRow    = context.getObj(LPInnerPointBatchOp.UPPER_BOUNDS);
        List<Row> lowerBoundsListRow    = context.getObj(LPInnerPointBatchOp.LOWER_BOUNDS);
        List<Row> unBoundsListRow       = context.getObj(LPInnerPointBatchOp.UN_BOUNDS);

        for(Row r:unBoundsListRow)
            bound[0][(int)r.getField(0)] = 1;
        for(Row r:upperBoundsListRow)
            bound[1][(int)r.getField(0)] = (double)r.getField(1);
        for(Row r:lowerBoundsListRow)
            bound[2][(int)r.getField(0)] = (double)r.getField(1);

        for(int i=0;i<n;i++){
            if(bound[0][i]==0 && bound[1][i]==MAX_VALUE && bound[2][i]==MIN_VALUE)
                continue;
            if(bound[0][i]==1)
                x_hat.set(i, x_hat.get(i)-x_hat.get(n+i));
            else
                if(bound[2][i]==MIN_VALUE)
                    x_hat.set(i, bound[1][i] - x_hat.get(i));
                else
                    x_hat.set(i, bound[2][i] + x_hat.get(i));
        }

        double result = 0.0;
        for(int i=0;i<n;i++)
            result += x_hat.get(i) * c[i];

        Row row = new Row(1);
        row.setField(0, result);
        RowCollector collector = new RowCollector();
        collector.collect(row);
        return collector.getRows();
    }
}
