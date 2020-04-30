package ThetaJoinTest.Skewed

import thetajoin.ThetaJoin

class SkewedAllComb extends Skewed {

  for (partitions <- 1 to 512 by 10) {
    for (condition <- IndexedSeq("<", ">")) {
      for (attrIndex1 <- 0 to 1; attrIndex2 <- 0 to 1) {
          val theta = new ThetaJoin(partitions)
          test(partitions.toString + " partitions, cond: " + condition + " attrIndex1: " + attrIndex1 + " attrIndex2: " + attrIndex2) {
            testSkewed(500, attrIndex1, attrIndex2)(theta, condition)
        }
      }
    }
  }

}
