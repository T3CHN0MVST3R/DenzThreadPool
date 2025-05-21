import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Пользовательская реализация пула потоков с расширенными возможностями.
 * 
 * Особенности:
 * - Настраиваемые параметры пула (corePoolSize, maxPoolSize, keepAliveTime, queueSize, minSpareThreads)
 * - Множественные очереди для уменьшения конкуренции
 * - Настраиваемые политики отказа
 * - Возможность выбора алгоритма распределения задач
 * - Подробное логирование всех операций
 */
public class CustomThreadPool implements CustomExecutor {
    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());
    
    // Основные параметры пула
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    
    // Компоненты пула
    private final List<Worker> workers;
    private final List<BlockingQueue<Runnable>> queues;
    private final CustomThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectionHandler;
    private final TaskDistributionStrategy distributionStrategy;
    
    // Состояние пула
    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;
    private final ReentrantLock mainLock = new ReentrantLock();
    private final AtomicInteger currentPoolSize = new AtomicInteger(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger completedTaskCount = new AtomicInteger(0);
    
    /**
     * Тип распределения задач между потоками
     */
    public enum DistributionType {
        ROUND_ROBIN,    // Распределение по кругу
        LEAST_LOADED    // Распределение в наименее загруженную очередь
    }
    
    /**
     * Тип обработки отказов
     */
    public enum RejectionPolicy {
        ABORT,              // Отклонить задачу с ошибкой
        CALLER_RUNS,        // Выполнить задачу в потоке вызывающего
        DISCARD,            // Просто отбросить задачу
        DISCARD_OLDEST      // Отбросить старейшую задачу в очереди
    }
    
    /**
     * Создает пул потоков с указанными параметрами и стратегиями по умолчанию.
     */
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                          TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        this(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, queueSize, minSpareThreads,
             DistributionType.ROUND_ROBIN, RejectionPolicy.ABORT);
    }
    
    /**
     * Создает пул потоков с указанными параметрами и стратегиями.
     */
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                          TimeUnit timeUnit, int queueSize, int minSpareThreads,
                          DistributionType distributionType, RejectionPolicy rejectionPolicy) {
        
        if (corePoolSize < 0 || maxPoolSize <= 0 || maxPoolSize < corePoolSize || 
            keepAliveTime < 0 || queueSize <= 0 || minSpareThreads < 0 || minSpareThreads > corePoolSize) {
            throw new IllegalArgumentException("Invalid thread pool parameters");
        }
        
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        
        this.workers = new ArrayList<>(maxPoolSize);
        this.queues = new ArrayList<>(maxPoolSize);
        this.threadFactory = new CustomThreadFactory();
        
        // Создаем стратегию распределения задач
        switch (distributionType) {
            case ROUND_ROBIN:
                this.distributionStrategy = new RoundRobinDistribution();
                break;
            case LEAST_LOADED:
                this.distributionStrategy = new LeastLoadedDistribution();
                break;
            default:
                this.distributionStrategy = new RoundRobinDistribution();
        }
        
        // Создаем обработчик отказов
        switch (rejectionPolicy) {
            case ABORT:
                this.rejectionHandler = new AbortPolicy();
                break;
            case CALLER_RUNS:
                this.rejectionHandler = new CallerRunsPolicy();
                break;
            case DISCARD:
                this.rejectionHandler = new DiscardPolicy();
                break;
            case DISCARD_OLDEST:
                this.rejectionHandler = new DiscardOldestPolicy();
                break;
            default:
                this.rejectionHandler = new AbortPolicy();
        }
        
        // Инициализируем пул
        initializePool();
        
        logger.info(String.format(
            "Thread pool initialized with parameters: corePoolSize=%d, maxPoolSize=%d, queueSize=%d, minSpareThreads=%d, " +
            "distributionType=%s, rejectionPolicy=%s", 
            corePoolSize, maxPoolSize, queueSize, minSpareThreads, distributionType, rejectionPolicy));
    }
    
    /**
     * Инициализирует пул созданием базового числа потоков.
     */
    private void initializePool() {
        mainLock.lock();
        try {
            for (int i = 0; i < corePoolSize; i++) {
                createWorker();
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Создает новый рабочий поток с соответствующей очередью задач.
     */
    private Worker createWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        queues.add(queue);
        Worker worker = new Worker(queue);
        workers.add(worker);
        Thread thread = threadFactory.newThread(worker);
        worker.thread = thread;
        thread.start();
        currentPoolSize.incrementAndGet();
        return worker;
    }
    
    /**
     * Проверяет, нужно ли создать дополнительные "резервные" потоки.
     */
    private void ensureMinimumSpareThreads() {
        int spareThreads = currentPoolSize.get() - activeThreads.get();
        if (spareThreads < minSpareThreads && currentPoolSize.get() < maxPoolSize) {
            int threadsToCreate = Math.min(minSpareThreads - spareThreads, maxPoolSize - currentPoolSize.get());
            for (int i = 0; i < threadsToCreate; i++) {
                createWorker();
                logger.fine("Created spare thread. Current pool size: " + currentPoolSize.get());
            }
        }
    }
    
    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException("Task cannot be null");
        }
        
        if (isShutdown) {
            rejectionHandler.rejectedExecution(task, null);
            logger.warning("[Rejected] Task was rejected because the pool is shutting down");
            return;
        }
        
        mainLock.lock();
        try {
            // Проверяем, нужно ли создать новые рабочие потоки
            ensureMinimumSpareThreads();
            
            // Находим целевую очередь на основе выбранной стратегии
            BlockingQueue<Runnable> targetQueue = distributionStrategy.getTargetQueue(queues);
            
            // Пытаемся добавить задачу в очередь
            if (!targetQueue.offer(task)) {
                // Если не удалось добавить в очередь, пытаемся создать новый поток (если возможно)
                if (currentPoolSize.get() < maxPoolSize) {
                    Worker worker = createWorker();
                    worker.queue.offer(task);
                    logger.info("[Pool] Created new thread for task due to queue overflow. Current pool size: " + currentPoolSize.get());
                } else {
                    // Если нельзя создать новый поток, используем обработчик отказов
                    rejectionHandler.rejectedExecution(task, null);
                    logger.warning("[Rejected] Task was rejected due to overload!");
                }
            } else {
                int queueIndex = queues.indexOf(targetQueue);
                logger.fine("[Pool] Task accepted into queue #" + queueIndex + ": " + task.toString());
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException();
        
        // Создаем FutureTask для выполнения Callable
        RunnableFuture<T> futureTask = new FutureTask<>(callable);
        
        // Отправляем задачу на выполнение
        execute(futureTask);
        
        return futureTask;
    }
    
    @Override
    public void shutdown() {
        logger.info("[Pool] Shutdown initiated");
        mainLock.lock();
        try {
            isShutdown = true;
            
            // Прерываем только неактивные потоки
            for (Worker worker : workers) {
                if (worker.isIdle.get() == 1) {
                    worker.interruptIfIdle();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public void shutdownNow() {
        logger.info("[Pool] Immediate shutdown initiated");
        mainLock.lock();
        try {
            isShutdown = true;
            
            // Прерываем все потоки
            for (Worker worker : workers) {
                worker.interruptNow();
            }
            
            // Очищаем все очереди
            for (BlockingQueue<Runnable> queue : queues) {
                queue.clear();
            }
            
            isTerminated = true;
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Проверяет, завершил ли пул свою работу.
     */
    public boolean isTerminated() {
        mainLock.lock();
        try {
            if (!isShutdown) {
                return false;
            }
            
            // Проверяем, остались ли активные потоки
            for (Worker worker : workers) {
                if (worker.thread.isAlive()) {
                    return false;
                }
            }
            
            isTerminated = true;
            return true;
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Ожидает завершения всех задач после вызова shutdown.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanos;
        
        mainLock.lock();
        try {
            while (!isTerminated()) {
                if (nanos <= 0) {
                    return false;
                }
                // Ждем указанное время
                TimeUnit.NANOSECONDS.sleep(Math.min(nanos, 100_000_000)); // Не более 100ms за раз
                nanos = deadline - System.nanoTime();
            }
            return true;
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Возвращает текущее количество активных потоков.
     */
    public int getActiveCount() {
        return activeThreads.get();
    }
    
    /**
     * Возвращает общее количество завершенных задач.
     */
    public long getCompletedTaskCount() {
        return completedTaskCount.get();
    }
    
    /**
     * Возвращает текущий размер пула потоков.
     */
    public int getPoolSize() {
        return currentPoolSize.get();
    }
    
    /**
     * Базовый интерфейс для стратегий распределения задач.
     */
    private interface TaskDistributionStrategy {
        BlockingQueue<Runnable> getTargetQueue(List<BlockingQueue<Runnable>> queues);
    }
    
    /**
     * Распределение задач по круговому принципу.
     */
    private class RoundRobinDistribution implements TaskDistributionStrategy {
        private final AtomicInteger nextQueueIndex = new AtomicInteger(0);
        
        @Override
        public BlockingQueue<Runnable> getTargetQueue(List<BlockingQueue<Runnable>> queues) {
            int index = nextQueueIndex.getAndIncrement() % queues.size();
            return queues.get(index);
        }
    }
    
    /**
     * Распределение задач в наименее загруженную очередь.
     */
    private class LeastLoadedDistribution implements TaskDistributionStrategy {
        @Override
        public BlockingQueue<Runnable> getTargetQueue(List<BlockingQueue<Runnable>> queues) {
            BlockingQueue<Runnable> leastLoadedQueue = queues.get(0);
            int minSize = leastLoadedQueue.size();
            
            for (BlockingQueue<Runnable> queue : queues) {
                int queueSize = queue.size();
                if (queueSize < minSize) {
                    minSize = queueSize;
                    leastLoadedQueue = queue;
                }
            }
            
            return leastLoadedQueue;
        }
    }
    
    /**
     * Рабочий поток для выполнения задач.
     */
    private class Worker implements Runnable {
        final BlockingQueue<Runnable> queue;
        Thread thread;
        volatile boolean running = true;
        final AtomicInteger idleCount = new AtomicInteger(0);
        final AtomicInteger tasksExecuted = new AtomicInteger(0);
        final AtomicInteger idleTimes = new AtomicInteger(0);
        final AtomicInteger busyTimes = new AtomicInteger(0);
        final AtomicInteger idleTimeouts = new AtomicInteger(0);
        final AtomicInteger interruptedTimes = new AtomicInteger(0);
        // Флаг, показывающий, что воркер сейчас простаивает
        final AtomicInteger isIdle = new AtomicInteger(0);
        
        public Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }
        
        /**
         * Прерывает поток, если он находится в режиме ожидания
         */
        public void interruptIfIdle() {
            if (isIdle.get() == 1) {
                thread.interrupt();
                logger.fine("[Worker] " + thread.getName() + " interrupted while idle");
                interruptedTimes.incrementAndGet();
            }
        }
        
        /**
         * Немедленно прерывает поток
         */
        public void interruptNow() {
            running = false;
            thread.interrupt();
            logger.fine("[Worker] " + thread.getName() + " interrupted immediately");
            interruptedTimes.incrementAndGet();
        }
        
        @Override
        public void run() {
            try {
                while (running) {
                    Runnable task = null;
                    try {
                        // Отмечаем, что воркер ожидает задачу
                        isIdle.set(1);
                        idleCount.incrementAndGet();
                        idleTimes.incrementAndGet();
                        
                        // Ожидаем задачу с таймаутом
                        task = queue.poll(keepAliveTime, timeUnit);
                        
                        // Больше не ожидаем
                        isIdle.set(0);
                        idleCount.decrementAndGet();
                        
                        if (task != null) {
                            busyTimes.incrementAndGet();
                            
                            // Выполняем задачу
                            executeTask(task);
                        } else if (currentPoolSize.get() > corePoolSize) {
                            // Если нет задачи и у нас больше потоков, чем corePoolSize, завершаем текущий поток
                            idleTimeouts.incrementAndGet();
                            logger.info("[Worker] " + thread.getName() + " idle timeout, stopping.");
                            break;
                        }
                    } catch (InterruptedException e) {
                        if (!running || isShutdown) {
                            // Если прерывание из-за shutdown, завершаем поток
                            break;
                        }
                    }
                }
            } finally {
                // Очистка при завершении потока
                mainLock.lock();
                try {
                    workers.remove(this);
                    currentPoolSize.decrementAndGet();
                    logger.info("[Worker] " + thread.getName() + " terminated.");
                } finally {
                    mainLock.unlock();
                }
            }
        }
        
        /**
         * Выполняет задачу с логированием и обработкой ошибок
         */
        private void executeTask(Runnable task) {
            activeThreads.incrementAndGet();
            try {
                logger.fine("[Worker] " + thread.getName() + " executes " + task);
                task.run();
                tasksExecuted.incrementAndGet();
                completedTaskCount.incrementAndGet();
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "[Worker] Error executing task in " + thread.getName(), t);
            } finally {
                activeThreads.decrementAndGet();
            }
        }
    }
    
    /**
     * Фабрика для создания потоков с именами.
     */
    private static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "MyPool-worker-";
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            // Не делаем потоки демонами, чтобы они могли завершить задачи перед выходом из JVM
            t.setDaemon(false);
            logger.info("[ThreadFactory] Creating new thread: " + t.getName());
            return t;
        }
    }
    
    /**
     * Политика отказа, которая выбрасывает исключение.
     */
    private static class AbortPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("[Rejected] Task rejected: " + r.toString());
            throw new RejectedExecutionException("Task " + r.toString() + " rejected");
        }
    }
    
    /**
     * Политика отказа, которая выполняет задачу в потоке вызывающего.
     */
    private static class CallerRunsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("[Rejected] Task " + r.toString() + " will be executed in caller thread");
            // Выполняем задачу в потоке вызывающего
            r.run();
        }
    }
    
    /**
     * Политика отказа, которая молча отбрасывает задачу.
     */
    private static class DiscardPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("[Rejected] Task " + r.toString() + " was silently discarded");
            // Ничего не делаем, просто игнорируем задачу
        }
    }
    
    /**
     * Политика отказа, которая отбрасывает самую старую задачу в очереди.
     */
    private static class DiscardOldestPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("[Rejected] Discarding oldest task to make room for: " + r.toString());
            if (executor == null || executor.getQueue() == null) {
                throw new RejectedExecutionException("Executor or queue is null");
            }
            
            if (!executor.isShutdown()) {
                // Удаляем старейшую задачу и добавляем новую
                executor.getQueue().poll();
                executor.execute(r);
            }
        }
    }
}
