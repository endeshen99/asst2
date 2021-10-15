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
                                                                            workers_ready(0) {
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i] = thread([this, i]() {
            worker(i);
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    cout << "deconstructing" << endl;
    task_lock.lock();
    while (!allWorkersIdle()) {
        checkWorkLeft.wait(task_lock);
    }
    task_lock.unlock();

    deconstruct = true;
    //cout << "notify threads" << endl;
    for (auto& cv : wakeThread) {
        cv.notify_all();
    }
//wakeAllThreads.notify_all();
    for (thread& t : thread_vec) {
        t.join();
    }
}

bool TaskSystemParallelThreadPoolSleeping::allWorkersIdle() {
    for (bool i : idle) {
        if (!i) {
            return false;
        }
    }
    return true;
}

void TaskSystemParallelThreadPoolSleeping::worker(int workerId){

    // TODO: adapt sleeping threads, ask workers to grab work from
    // and update processing_progress
    // and invoke task_finished subroutine

    //unique_lock<mutex> ulock(task_lock);
    task_lock.lock();
    //cout << workerId << " is about to start " << endl;
    workers_ready++;
    readyToStart.notify_all();
    wakeThread[workerId].wait(task_lock);
    task_lock.unlock();
    //cout << workerId << " is starting " << endl;
    while (!deconstruct) {
        task_lock.lock();
        if (cur_task == num_total_tasks) {
            idle[workerId] = true;
            //cout << "notify main thread done" << endl;
            checkWorkLeft.notify_all();
            //cout << workerId << " is sleeping" << endl;
            wakeThread[workerId].wait(task_lock);
            task_lock.unlock();
            if (deconstruct) {
                break;
            }
            continue;
        }

        int my_task = cur_task;
        //cout << workerId << " is running job " << my_task << endl;
        int my_total_tasks = num_total_tasks;
        cur_task++;
        task_lock.unlock();

        runnable->runTask(my_task, my_total_tasks);

    }
    //cout << workerId << " is done" << endl;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    task_lock.lock();
    //cout << "starting with " << total_tasks << endl;
    cur_task = 0;
    for (int i =0; i < idle.size(); i++) {
        idle[i] = false;
    }
    
    while (workers_ready != thread_vec.size()) {
        readyToStart.wait(task_lock);
    }
    task_lock.unlock();
    for (auto& cv : wakeThread) {
        cv.notify_all();
    }
    task_lock.lock();
    while (!allWorkersIdle()) {
        //cout << "main thread checking workd"<<endl;
        checkWorkLeft.wait(task_lock);
    }
    //cout << "main thread sees no more owrk"<<endl;
    task_lock.unlock();
    //cout << " run done" << endl;
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

    finished_task_count++;
    
    // deque this finished task
    processing_progress.erase(tid);

    // remove the finished task's runnable from our map
    id_to_task.erase(tid);

    dep_lock.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    dep_lock.lock();
    task_count++;
    TaskID curr_task_id = task_count;

    id_to_task[curr_task_id] = {runnable, num_total_tasks};

    // if the task has no dependencies, queue it to processing_progress
    if (deps.size() == 0) {
        processing_progress[curr_task_id] = {num_total_tasks, 0, 0};

    } else {

        // TODO: verify that we append an empty vector if there's no deps
        std::set<TaskID> existing_deps;
        
        for (const auto& dep_task_id: deps) {
            
            // filter tasks in the dependency list that have already finished
            if (deps_map.count(dep_task_id)) {
                existing_deps.insert(dep_task_id);

                // tell the tasks in the dependency list: this task depends on you.
                // so that when you are finished, you can notify this task so.
                //
                // Two situations:
                //   1. append to an existing vector 
                //   2. create a new vector with one element
                if (deps_map_inverse.count(dep_task_id)) {
                    deps_map_inverse[dep_task_id].insert(curr_task_id);
                } else {       // TODO: verify we create a one-element vector here, 
                    std::set<TaskID> deps_inverse = {curr_task_id};
                    deps_map_inverse[dep_task_id] = deps_inverse;
                }
            }
        }
        deps_map[curr_task_id] = existing_deps;
    }

    dep_lock.unlock();
    return curr_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
