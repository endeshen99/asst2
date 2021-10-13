#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads):
                                                                            ITaskSystem(num_threads),
                                                                            thread_vec(num_threads),
                                                                            task_count(0),
                                                                            finished_task_count(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSpinning::worker() {

    // TODO: adapt sleeping threads, ask workers to grab work
    // from the processing_queue, update processing_progress,
    // and invoke task_finished subroutine

    // while (true) {
    //     if (finished) break;
    //     task_lock.lock();
    //     if (num_total_tasks_curr == 0 || cur_task == num_total_tasks_curr) {
    //         task_lock.unlock();
    //         continue;
    //     }
    //     //cout << "current task: " << cur_task << "\n" << std::flush;
    //     int task_num = cur_task;
    //     cur_task++;
    //     task_lock.unlock();
    //     runnable_curr->runTask(task_num, num_total_tasks_curr);
    //     task_finished_lock.lock();
    //     finished_tasks++;
    //     task_finished_lock.unlock();
    // }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::task_finished(TaskID tid) {
    // when a task is finished, no tasks are dependent on it anymore
    // delete its record from deps_map, and delete its value from deps_map.values()
    // delete its record from deps_map_inverse
    // TODO

    finished_task_count++;

    // deque this finished task
    // queue any tasks that have zero dependencies into the processing_queue, and
    // update corresponding processing_progress
    // TODO


}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //


    task_count++;
    TaskID curr_task_id = task_count;

    id_to_task[curr_task_id] = {runnable, num_total_tasks};

    // TODO: want to append an empty vector if there's no deps. verify the behavior.
    std::vector<TaskID> existing_deps;
    dep_lock.lock();
    for (const auto& dep_task_id: deps) {
        // filter tasks in the dependency list that have already finished
        if (deps_map.count(dep_task_id)) {
            existing_deps.push_back(dep_task_id);

            // tell the tasks in the dependency list: this task depends on you.
            // so that when you are finished, you can notify this task so.
            //
            // Two situations:
            //   1. append to an existing vector 
            //   2. create a new vector with one element
            if (deps_map_inverse.count(dep_task_id)) {
                deps_map_inverse[dep_task_id].push_back(curr_task_id);
            } else {       // TODO: want to create a one-element vector here, verify
                std::vector<TaskID> deps_inverse = {curr_task_id};
                deps_map_inverse[dep_task_id] = deps_inverse;
            }
        }
    }
    deps_map[curr_task_id] = existing_deps;

    // if the task has no dependencies, queue it directly to the processing_queue, and
    // update corresponding processing_progress
    // TODO

    dep_lock.unlock();
    return curr_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
