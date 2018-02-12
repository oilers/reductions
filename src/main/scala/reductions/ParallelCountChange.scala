package reductions

import common.parallel
import org.scalameter._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    println("Calculating Sequential Result")
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    val seq = List(s"sequential result = $seqResult", s"sequential count time: $seqtime ms")

    def measureParallelCountChange(threshold: ParallelCountChange.Threshold): List[String] = {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      List(s"parallel result = $parResult", s"parallel count time: $fjtime ms", s"speedup: ${seqtime / fjtime}")
    }

    println("Calculating Money Threshold")
    val moneyResults = measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("Calculating Coin Threshold")
    val coinResult = measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    println("Calculating Combined Result")
    val combinedResult = measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
    println("Sequential Results:")
    println(seq mkString "\n")
    println("Money Threshold")
    println(moneyResults mkString "\n")
    println("Coin Threshold")
    println(coinResult mkString "\n")
    println("Combined Result")
    println(combinedResult mkString "\n")
  }
}

object ParallelCountChange {

  /** Returns the number of ways change can be made from the specified list of
    * coins for the specified amount of money.
    */
  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0) 1
    else if (money < 0 || coins.isEmpty) 0
    else countChange(money, coins.tail) + countChange(money - coins.head, coins)
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
    * specified list of coins for the specified amount of money.
    */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    if (money == 0) 1
    else if (money < 0 || coins.isEmpty) 0
    else if (threshold(money, coins))
      countChange(money, coins)
    else {
      val (left, right) = parallel(parCountChange(money, coins.tail, threshold), parCountChange(money - coins.head, coins, threshold))
      left + right
    }
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold = {
    val threshold = (startingMoney * 2) / 3
    (currMoney: Int, _) => currMoney <= threshold
  }

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold = {
    val threshold = (totalCoins * 2) / 3
    (_, coins) => coins.size <= threshold
  }


  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    val numCoins = allCoins.length
    (money, coins) => (money * coins.length) <= (startingMoney * numCoins) / 2
  }
}
