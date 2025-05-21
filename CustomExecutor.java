import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Интерфейс пользовательского пула потоков, расширяющий стандартный Executor.
 */
public interface CustomExecutor extends Executor {
    /**
     * Выполняет задачу в пуле потоков.
     *
     * @param command задача для выполнения
     */
    void execute(Runnable command);
    
    /**
     * Отправляет задачу на выполнение и возвращает Future с результатом.
     *
     * @param callable задача, возвращающая результат
     * @param <T> тип результата
     * @return Future с результатом выполнения
     */
    <T> Future<T> submit(Callable<T> callable);
    
    /**
     * Начинает упорядоченное завершение работы, при котором ранее отправленные задачи
     * выполняются, но новые задачи не принимаются.
     */
    void shutdown();
    
    /**
     * Пытается остановить все активно выполняющиеся задачи, останавливает обработку
     * ожидающих задач и возвращает список задач, ожидающих выполнения.
     */
    void shutdownNow();
}
