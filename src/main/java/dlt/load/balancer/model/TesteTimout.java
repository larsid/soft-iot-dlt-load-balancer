package dlt.load.balancer.model;

import java.time.Duration;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class TesteTimout {

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public TesteTimout() {}

  public void run() {
    final Duration timeout = Duration.ofSeconds(10);
    final Future handler = executor.submit(
      new Callable<String>() {
        @Override
        public String call() throws Exception {
          requestData();
          return "Tudo Certo";
        }
      }
    );

    try {
      handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      handler.cancel(true);
      System.out.println("Tempo esgotado");
      executor.shutdownNow();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  protected void requestData() {
    while (!executor.isShutdown());
  }

  public static void main(String[] args) {
    TesteTimout teste = new TesteTimout();
    teste.run();
  }
}
