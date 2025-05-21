import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;

/**
 * Демонстрационная программа для тестирования пользовательского пула потоков.
 * Выполняет различные сценарии для проверки функциональности и производительности.
 */
public class ThreadPoolDemo {
    private static final Logger logger = Logger.getLogger(ThreadPoolDemo.class.getName());
    private static final AtomicInteger completedTasks = new AtomicInteger(0);
    private static final AtomicInteger rejectedTasks = new AtomicInteger(0);

    public static void main(String[] args) {
        // Настройка логирования
        configureLogging();
        
        logger.info("=====================================================");
        logger.info("Демонстрация пользовательского пула потоков");
        logger.info("=====================================================");
        
        // Сценарий 1: Стандартное использование с Round-Robin распределением
        testScenario1();
        
        // Сценарий 2: Использование политики отказа CALLER_RUNS
        testScenario2();
        
        // Сценарий 3: Использование алгоритма Least-Loaded
        testScenario3();
        
        // Сценарий 4: Тестирование метода submit с Callable и Future
        testScenario4();
        
        // Сценарий 5: Тестирование минимального числа резервных потоков (minSpareThreads)
        testScenario5();
        
        logger.info("=====================================================");
        logger.info("Демонстрация завершена");
        logger.info("=====================================================");
    }
    
    /**
     * Настройка логирования для демонстрационной программы
     */
    private static void configureLogging() {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.FINE);
        
        // Удаляем существующие обработчики и добавляем свой
        for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }
        
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        handler.setFormatter(new SimpleFormatter() {
            @Override
            public String format(java.util.logging.LogRecord record) {
                return String.format("[%s] %s: %s%n",
                        record.getLevel().getName(),
                        Thread.currentThread().getName(),
                        record.getMessage());
            }
        });
        
        rootLogger.addHandler(handler);
    }
    
    /**
     * Сценарий 1: Стандартное использование с Round-Robin распределением
     */
    private static void testScenario1() {
        logger.info("\n\n=== Сценарий 1: Стандартное использование с Round-Robin ===");
        
        // Создаем пул с параметрами
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            4,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            5,  // queueSize
            1   // minSpareThreads
        );
        
        // Отправляем задачи
        logger.info("Отправляем 10 задач с задержкой 1 секунда...");
        resetCounters();
        submitTasks(pool, 10, 1000, "Сценарий1-");
        
        // Ждем выполнения задач
        waitForTasks(15);
        
        // Выводим статистику
        printStatistics(pool);
        
        // Корректное завершение пула
        shutdownAndWait(pool);
    }
    
    /**
     * Сценарий 2: Использование политики отказа CALLER_RUNS
     */
    private static void testScenario2() {
        logger.info("\n\n=== Сценарий 2: Использование политики отказа CALLER_RUNS ===");
        
        // Создаем пул с параметрами и политикой CALLER_RUNS
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            3,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            3,  // queueSize - маленький размер очереди для демонстрации отказов
            1,  // minSpareThreads
            CustomThreadPool.DistributionType.ROUND_ROBIN,
            CustomThreadPool.RejectionPolicy.CALLER_RUNS
        );
        
        // Отправляем много задач, чтобы вызвать отказы
        logger.info("Отправляем 15 задач с маленькой очередью для демонстрации отказов...");
        resetCounters();
        submitTasks(pool, 15, 2000, "Сценарий2-");
        
        // Ждем выполнения задач
        waitForTasks(30);
        
        // Выводим статистику
        printStatistics(pool);
        
        // Корректное завершение пула
        shutdownAndWait(pool);
    }
    
    /**
     * Сценарий 3: Использование алгоритма Least-Loaded
     */
    private static void testScenario3() {
        logger.info("\n\n=== Сценарий 3: Использование алгоритма Least-Loaded ===");
        
        // Создаем пул с параметрами и алгоритмом Least-Loaded
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            4,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            5,  // queueSize
            1,  // minSpareThreads
            CustomThreadPool.DistributionType.LEAST_LOADED, // Используем LEAST_LOADED
            CustomThreadPool.RejectionPolicy.ABORT
        );
        
        // Отправляем задачи с разным временем выполнения для демонстрации балансировки
        logger.info("Отправляем смешанные задачи для демонстрации балансировки...");
        resetCounters();
        
        // Сначала отправляем несколько длинных задач
        for (int i = 0; i < 3; i++) {
            final int taskId = i;
            submitTaskWithTimeout(pool, 5000, "Сценарий3-Длинная-" + taskId);
        }
        
        // Даем время на распределение длинных задач
        waitForTasks(2);
        
        // Затем отправляем короткие задачи, которые должны идти в менее загруженные очереди
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            submitTaskWithTimeout(pool, 1000, "Сценарий3-Короткая-" + taskId);
        }
        
        // Ждем выполнения задач
        waitForTasks(20);
        
        // Выводим статистику
        printStatistics(pool);
        
        // Корректное завершение пула
        shutdownAndWait(pool);
    }
    
    /**
     * Сценарий 4: Тестирование метода submit с Callable и Future
     */
    private static void testScenario4() {
        logger.info("\n\n=== Сценарий 4: Тестирование метода submit с Future ===");
        
        // Создаем пул с параметрами
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            4,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            5,  // queueSize
            1   // minSpareThreads
        );
        
        // Отправляем задачи с использованием submit
        logger.info("Отправляем 5 задач с использованием submit и получаем результаты через Future...");
        resetCounters();
        
        Future<?>[] futures = new Future[5];
        
        for (int i = 0; i < 5; i++) {
            final int taskId = i;
            futures[i] = pool.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    String taskName = "Сценарий4-" + taskId;
                    logger.info("Task " + taskName + " started");
                    
                    // Имитируем работу
                    Thread.sleep(2000);
                    
                    completedTasks.incrementAndGet();
                    logger.info("Task " + taskName + " completed");
                    
                    // Возвращаем результат
                    return taskId * 10;
                }
            });
        }
        
        // Получаем результаты через Future
        for (int i = 0; i < futures.length; i++) {
            try {
                Integer result = (Integer) futures[i].get();
                logger.info("Получен результат для задачи " + i + ": " + result);
            } catch (Exception e) {
                logger.severe("Ошибка при получении результата: " + e.getMessage());
            }
        }
        
        // Выводим статистику
        printStatistics(pool);
        
        // Корректное завершение пула
        shutdownAndWait(pool);
    }
    
    /**
     * Сценарий 5: Тестирование минимального числа резервных потоков (minSpareThreads)
     */
    private static void testScenario5() {
        logger.info("\n\n=== Сценарий 5: Тестирование minSpareThreads ===");
        
        // Создаем пул с увеличенным значением minSpareThreads
        CustomThreadPool pool = new CustomThreadPool(
            3,  // corePoolSize
            6,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            5,  // queueSize
            2   // minSpareThreads - требуется минимум 2 свободных потока
        );
        
        // Отправляем задачи
        logger.info("Отправляем 5 задач, что должно активировать 2 потока и оставить 1 свободный...");
        resetCounters();
        
        // Отправляем задачи, но меньше, чем corePoolSize
        for (int i = 0; i < 2; i++) {
            final int taskId = i;
            submitTaskWithTimeout(pool, 3000, "Сценарий5-" + taskId);
        }
        
        // Даем немного времени на старт задач
        waitForTasks(1);
        
        // Проверяем, создаются ли дополнительные потоки для обеспечения minSpareThreads
        logger.info("Текущее количество активных потоков: " + pool.getActiveCount());
        logger.info("Текущий размер пула: " + pool.getPoolSize());
        
        // Ждем выполнения задач
        waitForTasks(5);
        
        // Отправляем еще задачи, чтобы проверить реакцию пула
        logger.info("Отправляем еще 5 задач для проверки механизма резервных потоков...");
        for (int i = 0; i < 5; i++) {
            final int taskId = i;
            submitTaskWithTimeout(pool, 2000, "Сценарий5-Дополнительная-" + taskId);
        }
        
        // Ждем выполнения задач
        waitForTasks(10);
        
        // Выводим статистику
        printStatistics(pool);
        
        // Корректное завершение пула
        shutdownAndWait(pool);
    }
    
    /**
     * Отправляет задачи в пул с указанной задержкой
     */
    private static void submitTasks(CustomThreadPool pool, int count, int sleepTime, String prefix) {
        for (int i = 0; i < count; i++) {
            submitTaskWithTimeout(pool, sleepTime, prefix + i);
        }
    }
    
    /**
     * Отправляет одну задачу с указанным временем выполнения
     */
    private static void submitTaskWithTimeout(CustomThreadPool pool, int sleepTime, String taskName) {
        try {
            pool.execute(() -> {
                logger.info("Task " + taskName + " started");
                try {
                    Thread.sleep(sleepTime);
                    completedTasks.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warning("Task " + taskName + " was interrupted");
                }
                logger.info("Task " + taskName + " completed");
            });
        } catch (Exception e) {
            rejectedTasks.incrementAndGet();
            logger.warning("Task " + taskName + " was rejected: " + e.getMessage());
        }
    }
    
    /**
     * Ожидает указанное время для выполнения задач
     */
    private static void waitForTasks(int seconds) {
        try {
            logger.info("Ожидание " + seconds + " секунд...");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Выводит статистику выполнения задач
     */
    private static void printStatistics(CustomThreadPool pool) {
        logger.info("\nСтатистика выполнения:");
        logger.info("  Текущий размер пула: " + pool.getPoolSize());
        logger.info("  Активные потоки: " + pool.getActiveCount());
        logger.info("  Завершенные задачи (из пула): " + pool.getCompletedTaskCount());
        logger.info("  Завершенные задачи (из счетчика): " + completedTasks.get());
        logger.info("  Отклоненные задачи: " + rejectedTasks.get());
    }
    
    /**
     * Корректно завершает работу пула и ожидает завершения всех задач
     */
    private static void shutdownAndWait(CustomThreadPool pool) {
        logger.info("Завершение работы пула...");
        pool.shutdown();
        
        try {
            if (pool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.info("Пул успешно завершил работу.");
            } else {
                logger.warning("Тайм-аут ожидания завершения пула, вызываем shutdownNow().");
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.severe("Прерывание во время ожидания завершения пула.");
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Сбрасывает счетчики задач
     */
    private static void resetCounters() {
        completedTasks.set(0);
        rejectedTasks.set(0);
    }
}
