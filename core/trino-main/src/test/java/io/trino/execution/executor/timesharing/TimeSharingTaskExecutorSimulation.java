/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.executor.timesharing;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import io.trino.execution.executor.timesharing.SimulationController.TaskSpecification;
import io.trino.execution.executor.timesharing.SplitGenerators.AggregatedLeafSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.FastLeafSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.IntermediateSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.L4LeafSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.QuantaExceedingSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.SimpleLeafSplitGenerator;
import io.trino.execution.executor.timesharing.SplitGenerators.SlowLeafSplitGenerator;

import java.io.Closeable;
import java.time.Instant;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.execution.executor.timesharing.Histogram.fromContinuous;
import static io.trino.execution.executor.timesharing.Histogram.fromDiscrete;
import static io.trino.execution.executor.timesharing.SimulationController.TaskSpecification.Type.INTERMEDIATE;
import static io.trino.execution.executor.timesharing.SimulationController.TaskSpecification.Type.LEAF;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

public class TimeSharingTaskExecutorSimulation
        implements Closeable
{
    static void main()
            throws Exception
    {
        try (TimeSharingTaskExecutorSimulation simulator = new TimeSharingTaskExecutorSimulation()) {
            simulator.run();
        }
    }

    private final ListeningExecutorService submissionExecutor = listeningDecorator(newCachedThreadPool(threadsNamed(getClass().getSimpleName() + "-%s")));
    private final ScheduledExecutorService overallStatusPrintExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService runningSplitsPrintExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService wakeupExecutor = newScheduledThreadPool(32);

    private final TimeSharingTaskExecutor taskExecutor;
    private final MultilevelSplitQueue splitQueue;

    private TimeSharingTaskExecutorSimulation()
    {
        splitQueue = new MultilevelSplitQueue(2);
        taskExecutor = new TimeSharingTaskExecutor(36, 72, 3, 8, splitQueue, Ticker.systemTicker());
        taskExecutor.start();
    }

    @Override
    public void close()
    {
        submissionExecutor.shutdownNow();
        overallStatusPrintExecutor.shutdownNow();
        runningSplitsPrintExecutor.shutdownNow();
        wakeupExecutor.shutdownNow();
        taskExecutor.stop();
    }

    public void run()
            throws Exception
    {
        long start = System.nanoTime();
        scheduleStatusPrinter(start);

        SimulationController controller = new SimulationController(taskExecutor, TimeSharingTaskExecutorSimulation::printSummaryStats);

        // Uncomment one of these:
        // runExperimentOverloadedCluster(controller);
        // runExperimentMisbehavingQuanta(controller);
        // runExperimentStarveSlowSplits(controller);
        runExperimentWithinLevelFairness(controller);

        System.out.println("Stopped scheduling new tasks. Ending simulation..");
        controller.stop();
        close();

        SECONDS.sleep(5);

        System.out.println();
        System.out.println("Simulation finished at " + Instant.now() + ". Runtime: " + nanosSince(start));
        System.out.println();

        printSummaryStats(controller, taskExecutor);
    }

    private void runExperimentWithinLevelFairness(SimulationController controller)
            throws InterruptedException
    {
        /*
        Designed to simulate how Trino allocates resources to tasks at the same level of the
        feedback queue when there is large variance in accrued scheduled time.

        Desired result:
        Scheduling within levels should be fair - total accrued time should not affect what
        fraction of resources tasks are allocated as long as they are in the same level.
        */

        System.out.println("Level fairness experiment started.");

        TaskSpecification longLeafSpec = new TaskSpecification(INTERMEDIATE, "l4_long", OptionalInt.empty(), 2, 16, new SimpleLeafSplitGenerator(MINUTES.toNanos(4), SECONDS.toNanos(1)));
        controller.addTaskSpecification(longLeafSpec);

        TaskSpecification shortLeafSpec = new TaskSpecification(INTERMEDIATE, "l4_short", OptionalInt.empty(), 2, 16, new SimpleLeafSplitGenerator(MINUTES.toNanos(2), SECONDS.toNanos(1)));
        controller.addTaskSpecification(shortLeafSpec);

        controller.enableSpecification(longLeafSpec);
        controller.run();

        // wait until long tasks are all well into L4
        MINUTES.sleep(1);
        controller.runCallback();

        // start short leaf tasks
        controller.enableSpecification(shortLeafSpec);

        // wait until short tasks hit L4
        SECONDS.sleep(25);
        controller.runCallback();

        // now watch for L4 fairness at this point
        MINUTES.sleep(2);

        System.out.println("Level fairness experiment completed.");
    }

    private void scheduleStatusPrinter(long start)
    {
        overallStatusPrintExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.printf(
                        "%6s -- %4s splits (R: %2s  L: %3s  I: %3s  B: %3s  W: %3s  C: %5s)  |  %3s tasks (%3s %3s %3s %3s %3s)  |  Selections: %4s %4s %4s %4s %3s\n",
                        nanosSince(start),
                        taskExecutor.getTotalSplits(),
                        taskExecutor.getRunningSplits(),
                        taskExecutor.getTotalSplits() - taskExecutor.getIntermediateSplits(),
                        taskExecutor.getIntermediateSplits(),
                        taskExecutor.getBlockedSplits(),
                        taskExecutor.getWaitingSplits(),
                        taskExecutor.getCompletedSplitsLevel0() + taskExecutor.getCompletedSplitsLevel1() + taskExecutor.getCompletedSplitsLevel2() + taskExecutor.getCompletedSplitsLevel3() + taskExecutor.getCompletedSplitsLevel4(),
                        taskExecutor.getTasks(),
                        taskExecutor.getRunningTasksLevel0(),
                        taskExecutor.getRunningTasksLevel1(),
                        taskExecutor.getRunningTasksLevel2(),
                        taskExecutor.getRunningTasksLevel3(),
                        taskExecutor.getRunningTasksLevel4(),
                        (int) splitQueue.getSelectedCountLevel0().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel1().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel2().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel3().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel4().getOneMinute().getRate());
            }
            catch (Exception _) {
            }
        }, 1, 1, SECONDS);
    }

    private static void printSummaryStats(SimulationController controller, TimeSharingTaskExecutor taskExecutor)
    {
        Map<TaskSpecification, Boolean> specEnabled = controller.getSpecificationEnabled();

        ListMultimap<TaskSpecification, SimulationTask> completedTasks = controller.getCompletedTasks();
        ListMultimap<TaskSpecification, SimulationTask> runningTasks = controller.getRunningTasks();
        Set<SimulationTask> allTasks = ImmutableSet.<SimulationTask>builder().addAll(completedTasks.values()).addAll(runningTasks.values()).build();

        long completedSplits = completedTasks.values().stream().mapToInt(t -> t.getCompletedSplits().size()).sum();
        long runningSplits = runningTasks.values().stream().mapToInt(t -> t.getCompletedSplits().size()).sum();

        System.out.println("Completed tasks : " + completedTasks.size());
        System.out.println("Remaining tasks : " + runningTasks.size());
        System.out.println("Completed splits: " + completedSplits);
        System.out.println("Remaining splits: " + runningSplits);
        System.out.println();
        System.out.println("Completed tasks  L0: " + taskExecutor.getCompletedTasksLevel0());
        System.out.println("Completed tasks  L1: " + taskExecutor.getCompletedTasksLevel1());
        System.out.println("Completed tasks  L2: " + taskExecutor.getCompletedTasksLevel2());
        System.out.println("Completed tasks  L3: " + taskExecutor.getCompletedTasksLevel3());
        System.out.println("Completed tasks  L4: " + taskExecutor.getCompletedTasksLevel4());
        System.out.println();
        System.out.println("Completed splits L0: " + taskExecutor.getCompletedSplitsLevel0());
        System.out.println("Completed splits L1: " + taskExecutor.getCompletedSplitsLevel1());
        System.out.println("Completed splits L2: " + taskExecutor.getCompletedSplitsLevel2());
        System.out.println("Completed splits L3: " + taskExecutor.getCompletedSplitsLevel3());
        System.out.println("Completed splits L4: " + taskExecutor.getCompletedSplitsLevel4());

        Histogram<Long> levelsHistogram = fromContinuous(ImmutableList.of(
                MILLISECONDS.toNanos(0L),
                MILLISECONDS.toNanos(1_000),
                MILLISECONDS.toNanos(10_000L),
                MILLISECONDS.toNanos(60_000L),
                MILLISECONDS.toNanos(300_000L),
                HOURS.toNanos(1),
                DAYS.toNanos(1)));

        System.out.println();
        System.out.println("Levels - Completed Task Processed Time");
        levelsHistogram.printDistribution(
                completedTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getProcessedTimeNanos,
                Duration::succinctNanos,
                TimeSharingTaskExecutorSimulation::formatNanos);

        System.out.println();
        System.out.println("Levels - Running Task Processed Time");
        levelsHistogram.printDistribution(
                runningTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getProcessedTimeNanos,
                Duration::succinctNanos,
                TimeSharingTaskExecutorSimulation::formatNanos);

        System.out.println();
        System.out.println("Levels - All Task Wait Time");
        levelsHistogram.printDistribution(
                runningTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getTotalWaitTimeNanos,
                Duration::succinctNanos,
                TimeSharingTaskExecutorSimulation::formatNanos);

        System.out.println();
        System.out.println("Specification - Processed time");
        Set<String> specifications = runningTasks.values().stream().map(t -> t.getSpecification().getName()).collect(Collectors.toSet());
        fromDiscrete(specifications).printDistribution(
                allTasks,
                t -> t.getSpecification().getName(),
                SimulationTask::getProcessedTimeNanos,
                identity(),
                TimeSharingTaskExecutorSimulation::formatNanos);

        System.out.println();
        System.out.println("Specification - Wait time");
        fromDiscrete(specifications).printDistribution(
                allTasks,
                t -> t.getSpecification().getName(),
                SimulationTask::getTotalWaitTimeNanos,
                identity(),
                TimeSharingTaskExecutorSimulation::formatNanos);

        System.out.println();
        System.out.println("Breakdown by specification");
        System.out.println("##########################");
        for (TaskSpecification specification : specEnabled.keySet()) {
            List<SimulationTask> allSpecificationTasks = ImmutableList.<SimulationTask>builder()
                    .addAll(completedTasks.get(specification))
                    .addAll(runningTasks.get(specification))
                    .build();

            System.out.println(specification.getName());
            System.out.println("=============================");
            System.out.println("Completed tasks           : " + completedTasks.get(specification).size());
            System.out.println("In-progress tasks         : " + runningTasks.get(specification).size());
            System.out.println("Total tasks               : " + specification.getTotalTasks());
            System.out.println("Splits/task               : " + specification.getNumSplitsPerTask());
            System.out.println("Current required time     : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getScheduledTimeNanos).sum()));
            System.out.println("Completed scheduled time  : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getProcessedTimeNanos).sum()));
            System.out.println("Total wait time           : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getTotalWaitTimeNanos).sum()));

            System.out.println();
            System.out.println("All Tasks by Scheduled time - Processed Time");
            levelsHistogram.printDistribution(
                    allSpecificationTasks,
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getProcessedTimeNanos,
                    Duration::succinctNanos,
                    TimeSharingTaskExecutorSimulation::formatNanos);

            System.out.println();
            System.out.println("All Tasks by Scheduled time - Wait Time");
            levelsHistogram.printDistribution(
                    allSpecificationTasks,
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getTotalWaitTimeNanos,
                    Duration::succinctNanos,
                    TimeSharingTaskExecutorSimulation::formatNanos);

            System.out.println();
            System.out.println("Complete Tasks by Scheduled time - Wait Time");
            levelsHistogram.printDistribution(
                    completedTasks.get(specification),
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getTotalWaitTimeNanos,
                    Duration::succinctNanos,
                    TimeSharingTaskExecutorSimulation::formatNanos);
        }
    }

    private static String formatNanos(List<Long> list)
    {
        LongSummaryStatistics stats = list.stream().mapToLong(value -> value).summaryStatistics();
        return format(
                "Min: %8s  Max: %8s  Avg: %8s  Sum: %8s",
                succinctNanos(stats.getMin() == Long.MAX_VALUE ? 0 : stats.getMin()),
                succinctNanos(stats.getMax() == Long.MIN_VALUE ? 0 : stats.getMax()),
                succinctNanos((long) stats.getAverage()),
                succinctNanos(stats.getSum()));
    }
}
