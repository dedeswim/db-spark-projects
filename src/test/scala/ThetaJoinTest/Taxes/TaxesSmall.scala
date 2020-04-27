package ThetaJoinTest.Taxes

class TaxesSmall extends Taxes {
  test("ThetaJoin.ineq_joinTestTax1k1PartitionLarger") {
    testTaxes(1000)(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestTax1k1PartitionSmaller") {
    testTaxes(1000)(thetaJoin1, "<")
  }

  test("ThetaJoin.ineq_joinTestTax1k2PartitionsLarger") {
    testTaxes(1000)(thetaJoin2, ">")
  }

  test("ThetaJoin.ineq_joinTestTax1k2PartitionsSmaller") {
    testTaxes(1000)(thetaJoin2, "<")
  }

  test("ThetaJoin.ineq_joinTestTax1k16PartitionsLarger") {
    testTaxes(1000)(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestTax1k16PartitionsSmaller") {
    testTaxes(1000)(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestTax1k128PartitionsLarger") {
    testTaxes(1000)(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestTax1k128PartitionsSmaller") {
    testTaxes(1000)(thetaJoin128, "<")
  }
}
