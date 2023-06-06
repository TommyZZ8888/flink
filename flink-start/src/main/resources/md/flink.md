Flink系统主要由两个组件组成：JobManager和TaskManager,Flink遵循Master-Slave架构设计原则。
JobManager为Master节点，TaskManager为Worker（Slaver）节点。

Flink在运行时又分为三种角色：JobManager,TaskManager,Client

JobManger
JobManager :负责整个 Flink 集群任务的调度以及资源的管理从客户端中接收作业

详细解释：客户端通过将编写好的 Flink 应用编译打包，提交到 JobManager，JobManger根据集群TaskManager 上 TaskSlot 的使用情况，为提交的应用分配相应的 TaskSlot 资源并命令 TaskManager 启动与执行从客户端中获取的作业；JobManger还负责协调Checkpoint 操作，每个 TaskManager 节点 收到 Checkpoint 触发指令后，完成 Checkpoint 操作，所有的 Checkpoint 协调过程都是在 Fink JobManager 中完成。

** >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> **

TaskManager
TaskManager: 负责具体的任务执行和任务资源申请和管理

详细解释：TaskManger从 JobManager 接收需要执行的任务，然后申请Slot 资源（根据集群Slot使用情况以及并行度设置）并尝试启动Task，开始执行作业,TaskManager中最小的资源调度单位是TaskSlots。TaskManger数量由集群中Slave数量决定

** >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> **

TaskSlots
TaskSlots：Task共享系统资源(内存);TaskManger并发执行能力决定性因素。

详细解释：TaskSlots：TaskManager 是一个 JVM 进程，是实际负责执行计算的Worker，会以独立的线程来执行一个task或多个subtask。为了控制一个 TaskManager 能执行多少个 task，Flink 提出了 Task Slot 的概念

Client
Client：Flink程序提交的客户端

Client是Flink程序提交的客户端，当用户提交一个Flink程序时，会首先创建一个Client，该Client首先会对用户提交的Flink程序进行预处理，并提交到Flink集群中处理。正因为其需要提交到Flink集群，所以Client需要从用户提交的Flink程序配置中获取JobManager的地址，并建立到JobManager的连接，将Flink Job提交给JobManager
