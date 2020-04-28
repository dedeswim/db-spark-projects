package ThetaJoinTest.Taxes

import thetajoin.ThetaJoin

class TaxesAllComb extends Taxes {

  for (partitions <- 1 to 512) {
    for (condition <- IndexedSeq("<", ">")) {
      val theta = new ThetaJoin(partitions)
      test(partitions.toString + " partitions, cond: " + condition) {
        testTaxes(500)(theta, condition)
      }
    }
  }

}
