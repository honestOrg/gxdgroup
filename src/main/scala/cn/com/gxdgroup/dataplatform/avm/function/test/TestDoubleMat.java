package cn.com.gxdgroup.dataplatform.avm.function.test;
import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.linalg.Algebra;
/**
 * Created by ThinkPad on 14-7-10.
 */
public class TestDoubleMat {
    public static void main(String[] args)
    {
        DoubleMatrix1D matrix1D;
       // matrix = new DenseDoubleMatrix1D(3,4);
        //DenseDoubleMatrix1D代表的是一维的向量，长度是3
        matrix1D = new DenseDoubleMatrix1D(3);
        System.out.println("填充");
        matrix1D.assign(new double[]{1, 2, 3});
        System.out.println(matrix1D);



        //&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        DoubleMatrix2D matrix;
        matrix = new DenseDoubleMatrix2D(3,4);
        //matrix = new SparseDoubleMatrix2D(3,4); // 稀疏矩阵
        //matrix = new RCDoubleMatrix2D(3,4); // 稀疏行压缩矩阵
        System.out.println("初始矩阵");
        System.out.println(matrix);
        System.out.println("填充");
        matrix.assign(new double[][]{{1, 2, 3, 4},{5, 6, 7, 8},{9, 10, 11, 12}});
        System.out.println(matrix);
        System.out.println("matrix.get(2,3):"+matrix.get(0,0));
        System.out.println("转置");
        DoubleMatrix2D transpose = Algebra.DEFAULT.transpose(matrix);
        System.out.println(transpose);
        System.out.println("矩阵乘法");
        System.out.println(Algebra.DEFAULT.mult(matrix, transpose));
    }
}
