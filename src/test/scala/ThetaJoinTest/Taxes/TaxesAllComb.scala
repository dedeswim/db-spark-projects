package ThetaJoinTest.Taxes

import thetajoin.ThetaJoin

class TaxesAllComb extends Taxes {

  for (partitions <- 1 to 512 by 5) {
    for (condition <- IndexedSeq("<", ">")) {
      for (attrIndex1 <- 1 to 2; attrIndex2 <- 1 to 2) {
        val theta = new ThetaJoin(partitions)
        test(partitions.toString + " partitions, cond: " + condition + " attrIndex1: " + attrIndex1 + " attrIndex2: " + attrIndex2) {
          testTaxes(500, attrIndex1, attrIndex2)(theta, condition)
        }
      }
    }
  }

}
