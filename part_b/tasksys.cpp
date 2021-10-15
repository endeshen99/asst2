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

void TaskSystemParallelThreadPoolSleeping::worker(int workerId){
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), thread_vec(num_threads), cur_task(0), num_total_tasks(0), idle(num_threads, true), deconstruct(false), wakeThread(num_threads), task_lock(), workers_ready(0) {
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

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* run, int total_tasks) {

    task_lock.lock();
    //cout << "starting with " << total_tasks << endl;
    cur_task = 0;
    num_total_tasks = total_tasks;
    runnable = run;
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

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
