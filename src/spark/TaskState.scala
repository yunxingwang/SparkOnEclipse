package spark


private[spark] object TaskState
  extends Enumeration("LAUNCHING", "RUNNING", "FINISHED", "FAILED", "KILLED", "LOST") {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  type TaskState = Value

  def isFinished(state: TaskState) = Seq(FINISHED, FAILED, LOST).contains(state)

}
