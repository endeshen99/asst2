#include "tasksys.h"

#include <iostream>
using namespace std;
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
                                                                            cur_task(0),
                                                                            num_total_tasks(0),
                                                                            idle(num_threads, true),
                                                                            deconstruct(false),
                                                                            wakeThread(num_threads),
                                                                            task_lock(),
                                                                            task_count(0),
                                                                            all_done(true),
                                                                            ready_to_start(false) {
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i] = thread([this, i]() {
            worker(i);
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    task_lock.lock();
    while (!allWorkersIdle()) {
        checkWorkLeft.wait(task_lock);
    }
    task_lock.unlock();

    deconstruct = true;
    for (auto& cv : wakeThread) {
        cv.notify_all();
    }
    for (thread& t : thread_vec) {
        t.join();
    }
}

bool TaskSystemParallelThreadPoolSleeping::allWorkersIdle() {
    for (bool b : idle) {
        if (!b) {
            return false;
        }
    }
    return true;
}
void TaskSystemParallelThreadPoolSleeping::worker(int workerId){
    task_lock.lock();
    while (!ready_to_start) {
        wakeThread[workerId].wait(task_lock);
    }
    task_lock.unlock();
    while (!deconstruct) {
        task_lock.lock();
        if (cur_task == num_total_tasks) {
            idle[workerId] = true;
            if (!all_done && allWorkersIdle()) {
                task_finished(cur_tid);
            } else {
                wakeThread[workerId].wait(task_lock);
            }
            task_lock.unlock();
            if (deconstruct) {
                break;
            }
            continue;
        }

        int my_task = cur_task;
        int my_total_tasks = num_total_tasks;
        cur_task++;
        task_lock.unlock();

        runnable->runTask(my_task, my_total_tasks);

    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* run, int total_tasks) {
    const vector<TaskID> no_deps{};
    runAsyncWithDeps(run, total_tasks,no_deps);
    sync();
}


void TaskSystemParallelThreadPoolSleeping::addRunnable(IRunnable* run, int total_tasks) {
    cur_task = 0;
    num_total_tasks = total_tasks;
    runnable = run;
    ready_to_start = true;
    for (int i =0; i < idle.size(); i++) {
        idle[i] = false;
    }
    for (auto& cv : wakeThread) {
        cv.notify_all();
    }
}



void TaskSystemParallelThreadPoolSleeping::task_finished(TaskID tid) {
    // when a task is finished, no tasks are dependent on it anymore
    // delete its record from deps_map, and delete its value from deps_map.values()
    // delete its record from deps_map_inverse


    dep_lock.lock();
    deps_map.erase(tid);
    for (const TaskID id_to_delete: deps_map_inverse[tid]) {
        deps_map[id_to_delete].erase(tid);

        // queue any tasks that have zero dependencies as a result into processing_progress
        if (deps_map[id_to_delete].size() == 0) {
            processing_progress[id_to_delete] = {id_to_task[id_to_delete].second, 0, 0};
        }
    }
    deps_map_inverse.erase(tid);
    
    // deque this finished task
    processing_progress.erase(tid);

    // remove the finished task's runnable from our map
    id_to_task.erase(tid);

    if (processing_progress.size() != 0) {
        // cout << "there are tasks to be processed" << endl;
        for (auto curr_task: processing_progress) {
            TaskID id = curr_task.first;
            array<int, 3> task = curr_task.second;
            if (task[1] == 0) {
                // cout << "found another task to do" << endl;
                cur_tid = id;
                // cout << "starting " << cur_tid<<endl;
                addRunnable(id_to_task[id].first, id_to_task[id].second);
                break;
            }
        }
    } else {
        all_done = true;
    }
    
    
    dep_lock.unlock();
    if (deps_map.size() == 0) {
        checkWorkLeft.notify_all();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {



    dep_lock.lock();
    task_count++;
    TaskID curr_task_id = task_count;

    id_to_task[curr_task_id] = {runnable, num_total_tasks};

    std::set<TaskID> valid_deps;
    
    for (const auto& dep_task_id: deps) {
        
        // deps might contain tasks that have already finished, we should ignore these
        // check deps_map to make sure each task has not finished
        if (deps_map.count(dep_task_id)) {
            valid_deps.insert(dep_task_id);

            // helper data structure for a faster update on deps_map when tasks are finished
            // otherwise has to iterate through deps_map to delete tasks
            if (deps_map_inverse.count(dep_task_id)) {
                deps_map_inverse[dep_task_id].insert(curr_task_id);
            } else {
                std::set<TaskID> deps_inverse = {curr_task_id};
                deps_map_inverse[dep_task_id] = deps_inverse;
            }
        }
    }
    deps_map[curr_task_id] = valid_deps;

    // if the task has no valid dependencies, queue it to processing_progress
    if (valid_deps.size() == 0) {
        processing_progress[curr_task_id] = {num_total_tasks, 0, 0};
    }
    all_done = false;
    // thread_waiting.notify_all();
    if (allWorkersIdle()) {
        cur_tid = curr_task_id;
        // cout << "starting " << cur_tid<<endl;
        addRunnable(runnable, num_total_tasks);
    }
    dep_lock.unlock();
    
    return curr_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    dep_lock.lock();
    while (deps_map.size() != 0) {
        checkWorkLeft.wait(dep_lock);
    }
    dep_lock.unlock();

    return;
}