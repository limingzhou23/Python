class PriorityQueue:

    def __init__(self):
        self.queue = []
        # 互斥锁
        self.mutex = Lock()
        # 条件对象
        self.more = Condition(self.mutex)

    def get(self):
        # 先加互斥锁
        with self.mutex:
            while True:
                if not self.queue:
                    # 堆为空则等待条件变量
                    # wait内部将释放互斥锁
                    self.more.wait()

                    # 条件变量满足，线程重新唤醒
                    # wait返回前，重新拿到互斥锁
                    # 睡眠这段时间，至少有一个生产者压入新任务
                    # 新任务有可能被其他线程抢走，因此需要重新检查堆是否为空
                    continue

                # 检查堆顶任务
                job_item = self.queue[0]

                # 判断执行时间是否到达
                now = time.time()
                executing_ts = job_item.executing_ts
                if executing_ts > now:
                    # 执行时间未到，则继续等待，直到时间达到或者有生产者压入新任务
                    self.more.wait(executing_ts - now)
                    # 有新任务提交或者等待时间已到，重新检查堆状态
                    continue

                # 弹出堆顶元素并返回
                heappop(self.queue)
                return job_item

    def put(self, job_item):
        with self.mutex:
            # 将任务压入堆内，堆以执行时间排序
            heappush(self.queue, job_item)
            # 唤醒等待条件对象的其他线程
            self.more.notify()