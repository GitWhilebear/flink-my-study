package streaming
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object WindowAggFuncExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text = env.socketTextStream("localhost", 7777)

    val counts = text.flatMap{ _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map{ (_, 1) }
      .keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .sum(1)

    counts.print()

    env.execute("WindowAggFuncExample")
  }
}
