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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), thread_vec(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    cur_task = 0;
}
void TaskSystemParallelSpawn::worker(IRunnable* runnable, int num_total_tasks) {
    task_lock.lock();
    while (cur_task != num_total_tasks) {
        int task_num = cur_task;
        cur_task++;
        task_lock.unlock();
        runnable->runTask(task_num, num_total_tasks);
        task_lock.lock();
    }
    task_lock.unlock();
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i] = thread([this, &runnable, &num_total_tasks]() {
              worker(runnable, num_total_tasks);
            });
    }
    // for (thread &t: thread_vec){ 
    //     t.join();
    // }
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i].join();
    }
    cur_task = 0;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

void TaskSystemParallelThreadPoolSpinning::worker() {
    while (true) {
        if (finished) break;
        task_lock.lock();
        if (num_total_tasks_curr == 0 || cur_task == num_total_tasks_curr) {
            task_lock.unlock();
            continue;
        }
        //cout << "current task: " << cur_task << "\n" << std::flush;
        int task_num = cur_task;
        cur_task++;
        task_lock.unlock();
        runnable_curr->runTask(task_num, num_total_tasks_curr);
        task_finished_lock.lock();
        finished_tasks++;
        task_finished_lock.unlock();
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), 
                                                                                             thread_vec(num_threads),
                                                                                             num_total_tasks_curr(0),
                                                                                             cur_task(0),
                                                                                             finished_tasks(0),
                                                                                             finished(false){
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i] = thread([this]() {
            worker();
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    finished = true;
    //cout << "ending" << endl;
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    //cout << "numer of total tasks: " << num_total_tasks << "\n" << std::flush;
    task_lock.lock();
    runnable_curr = runnable;
    num_total_tasks_curr = num_total_tasks;
    task_lock.unlock();
    while(true) {
        task_lock.lock();
        if (finished_tasks == num_total_tasks_curr) {
            num_total_tasks_curr = 0;
            cur_task = 0;
            finished_tasks = 0;
            task_lock.unlock();
            break;
        }
        task_lock.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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
    unique_lock<mutex> ulock(task_lock);
    cout << "locked" << endl;
    wakeThread[workerId].wait(ulock);
    cout << workerId << "wakes" << endl;
    while (true) {
        cout << workerId << " is before lock" << endl;
        //task_lock.lock();
        cout << "worker locked" << endl;
        cout << workerId << " is starting loop" << endl;
        if (cur_task == num_total_tasks) {
            cout << workerId << " is in if" << endl;
            //idle_lock.lock();
            idle[workerId] = true;
            //idle_lock.unlock();
            cout << workerId << " is sleeping" << endl;
            //task_lock.lock();
            task_lock.lock();
            wakeThread[workerId].wait(task_lock);
            cout << workerId << " is waking" << endl;
            if (deconstruct) {
                cout << workerId << " is deconstructing" << endl;
                break;
            }
            continue;
        }

        int my_task = cur_task;
        cout << workerId << " is running job " << my_task << endl;
        int my_total_tasks = num_total_tasks;
        cur_task++;
        //task_lock.unlock();

        runnable->runTask(my_task, my_total_tasks);

        checkWorkLeft.notify_all();
    }
    cout << workerId << " is done" << endl;
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), thread_vec(num_threads), cur_task(0), num_total_tasks(0), idle(num_threads, true), deconstruct(false), wakeThread(num_threads), task_lock() {
    for (int i = 0; i < thread_vec.size(); i++) {
        thread_vec[i] = thread([this, i]() {
            worker(i);
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    cout << "call" << endl;
    task_lock.lock();
    cout << "locked" << endl;
    cout << "start deonstruct" << endl;
    if (!allWorkersIdle()) {
        checkWorkLeft.wait(task_lock);
    }

    deconstruct = true;
    cout << "notify deonstruct" << endl;
//wakeAllThreads.notify_all();
    cout << "wait join" << endl;
    for (thread& t : thread_vec) {
        t.join();
    }

}
bool TaskSystemParallelThreadPoolSleeping::allWorkersIdle() {
    cout << "checking idle" << endl;
    for (bool i : idle) {
        if (!i) {
            return false;
        }
    }
    return true;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* run, int total_tasks) {

    cout << "called with " << total_tasks << endl;
    //task_lock.lock();
    cout << "locked" << endl;
    cout << "starting with " << total_tasks << endl;
    cur_task = 0;
    num_total_tasks = total_tasks;
    runnable = run;
    //idle_lock.lock();
    for (int i =0; i < idle.size(); i++) {
        idle[i] = false;
    }
    //idle_lock.unlock();
    cout << "main thread unlocking" << endl;
    //task_lock.unlock();
    for (auto& cv : wakeThread) {
        cout << "notify all wke up" << endl;
        cv.notify_all();
    }
    cout << "run before lock" << endl;
    //task_lock.unlock();
    if (!allWorkersIdle()) {
        cout << "locked" << endl;
        task_lock.lock();
        cout << "give lock back" << endl;
        checkWorkLeft.wait(task_lock);
    } else {
        // UNCOMMENT
        //task_lock.unlock();
    }
    cout << " run done" << endl;
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
