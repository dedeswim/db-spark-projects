package ThetaJoinTest.Skewed

import thetajoin.ThetaJoin

class SkewedAllComb extends Skewed {

  for (partitions <- 1 to 512) {
    for (condition <- IndexedSeq("<", ">")) {
      val theta = new ThetaJoin(partitions)
      test(partitions.toString + " partitions, cond: " + condition) {
        testSkewed(500)(theta, condition)
      }
    }
  }

}
