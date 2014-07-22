package cn.com.gxdgroup.dataplatform.avm.function.test
import scala.math.sqrt

import cern.colt.matrix._
import cern.colt.matrix.linalg._
import cern.jet.math._
/**
* Created by ThinkPad on 14-7-10.
*/
object LocalALS {
  var M = 0 // Number of movies 3
  var U = 0 // Number of users 4
  var F = 0 // Number of features 2
  var ITERATIONS = 0//5

  val LAMBDA = 0.01 // Regularization coefficient

  // Some COLT objects
  val factory2D = DoubleFactory2D.dense
  val factory1D = DoubleFactory1D.dense
  val algebra = Algebra.DEFAULT
  val blas = SeqBlas.seqBlas

  def generateR(): DoubleMatrix2D = {
    val mh = factory2D.random(M, F)
    val uh = factory2D.random(U, F)

    println("mh:"+mh)
    println("uh:"+uh)
    println("unkown:"+ factory1D.random(F))
    algebra.mult(mh, algebra.transpose(uh))
  }

  def rmse(targetR: DoubleMatrix2D, ms: Array[DoubleMatrix1D],
           us: Array[DoubleMatrix1D]): Double =
  {
    val r = factory2D.make(M, U)
    for (i <- 0 until M; j <- 0 until U) {
      r.set(i, j, blas.ddot(ms(i), us(j)))
    }
    blas.daxpy(-1, targetR, r)
    val sumSqs = r.aggregate(Functions.plus, Functions.square)
    sqrt(sumSqs / (M * U))
  }

  def updateMovie(i: Int, m: DoubleMatrix1D, us: Array[DoubleMatrix1D],
                  R: DoubleMatrix2D) : DoubleMatrix1D =
  {
    val XtX = factory2D.make(F, F)
    val Xty = factory1D.make(F)
    // For each user that rated the movie
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      //注意blas.dger(1, u, u, XtX)的方法因为是for循环，XtX有一个矩阵累加的过程累加的结果赋给XtX，主要是因为blas.dger
      blas.dger(1, u, u, XtX)
      println("*****u:"+j+"+++++++++++++:"+u)
      println("blas.dger(1, u, u, XtX):"+XtX)
      // Add u * rating to Xty
      //R.get(i, j)是取得这个矩阵的i行j列元素
      //注意blas.daxpy(R.get(i, j), u, Xty)的方法因为是for循环，Xty有一个矩阵累加的过程累加的结果赋给Xty，主要是因为blas.daxpy
      blas.daxpy(R.get(i, j), u, Xty)
      println("R.get(i, j):"+R.get(i, j)+"|"+"u:"+u+"|"+"Xty:"+Xty)
    }
    println("XtXfianl:"+XtX)
    println("Xtyfianl:"+Xty)

    // Add regularization coefs to diagonal terms
    for (d <- 0 until F) {
      XtX.set(d, d, XtX.get(d, d) + LAMBDA * U)
    }
    // Solve it with Cholesky
    val ch = new CholeskyDecomposition(XtX)
    val Xty2D = factory2D.make(Xty.toArray, F)
    val solved2D = ch.solve(Xty2D)
    solved2D.viewColumn(0)
  }

  def updateUser(j: Int, u: DoubleMatrix1D, ms: Array[DoubleMatrix1D],
                 R: DoubleMatrix2D) : DoubleMatrix1D =
  {
    val XtX = factory2D.make(F, F)
    val Xty = factory1D.make(F)
    // For each movie that the user rated
    for (i <- 0 until M) {
      val m = ms(i)
      // Add m * m^t to XtX
      blas.dger(1, m, m, XtX)
      // Add m * rating to Xty
      blas.daxpy(R.get(i, j), m, Xty)
    }
    // Add regularization coefs to diagonal terms
    for (d <- 0 until F) {
      XtX.set(d, d, XtX.get(d, d) + LAMBDA * M)
    }
    // Solve it with Cholesky
    val ch = new CholeskyDecomposition(XtX)
    val Xty2D = factory2D.make(Xty.toArray, F)
    val solved2D = ch.solve(Xty2D)
    solved2D.viewColumn(0)
  }

  def main(args: Array[String]) {
   val  okArray:Array[String] =Array("3","4","2","5")
    okArray match {
      case Array(m, u, f, iters) => {
        M = m.toInt
        U = u.toInt
        F = f.toInt
        ITERATIONS = iters.toInt
      }
      case _ => {
        System.err.println("Usage: LocalALS <M> <U> <F> <iters>")
        System.exit(1)
      }
    }
    printf("Running with M=%d, U=%d, F=%d, iters=%d\n", M, U, F, ITERATIONS)

    val R = generateR()

    // Initialize m and u randomly M=3,U=4
    var ms = Array.fill(M)(factory1D.random(F))

    var us = Array.fill(U)(factory1D.random(F))
println("ms$$$$$$$$$$$$$$$$:"+ms(1))
    println("us$$$$$$$$$$$$$$$$0:"+us(0))
    println("us$$$$$$$$$$$$$$$$1:"+us(1))
    println("us$$$$$$$$$$$$$$$$2:"+us(2))
    println("us$$$$$$$$$$$$$$$$3:"+us(3))

    // Iteratively update movies then users
    for (iter <- 1 to ITERATIONS) {
      println("Iteration " + iter + ":")
      ms = (0 until M).map(i => updateMovie(i, ms(i), us, R)).toArray
      us = (0 until U).map(j => updateUser(j, us(j), ms, R)).toArray
      println("RMSE = " + rmse(R, ms, us))
      println()
    }
  }
}
