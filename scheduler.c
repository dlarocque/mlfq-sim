#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum cpu_task_type {
    short_task,
    medium_task,
    long_task,
    io_task,
    NUM_TASK_TYPES
};

typedef struct Task {
    char *task_name;
    enum cpu_task_type task_type;
    int task_length;
    int odds_of_io;
    int priority;
    int time_remaining;
    struct timespec arrival_time;
    struct timespec first_cpu_time;
    struct timespec completion_time;
    bool ran_before;
    bool active;
    pthread_mutex_t mutex;
} task;

#define MLFQ_TOP_PRIORITY 3
#define QUANTUM_LEN 50
#define MAX_TIME_ALLOTMENT 200
#define MAX_TASKS 100
#define MAX_CPUS 100
struct MLFQ {
    unsigned int s;
    struct timespec last_reset_time;
    task *levels[MLFQ_TOP_PRIORITY][MAX_TASKS];
    task *active[MAX_TASKS];
    task *done[MAX_TASKS];
    int num_tasks[MLFQ_TOP_PRIORITY];
    int num_active;
    int num_done;
};

static void microsleep(unsigned int);

void mlfq_init(int);

void mlfq_introduce(task *);

void mlfq_insert(task *);

void append_task(task *[MAX_TASKS], task *, int *, pthread_mutex_t *);

void remove_task(task *, task *[MAX_TASKS], int *, pthread_mutex_t *);

task *pop_task(task *task_arr[MAX_TASKS], int *num_tasks, pthread_mutex_t *mutex);

task *mlfq_next();

void mlfq_done(task *);

bool mlfq_is_done();

void mlfq_reset_queues();

void *reader(void *);

void *scheduler(void *);

void *worker(void *);

void print_task(task *);

void verify_task(task *);

struct timespec diff(struct timespec, struct timespec);

void report_statistics();

long timespec_to_usecs(struct timespec);

int num_cpus;
char *filename;
struct MLFQ *mlfq;
bool reading_done = false;
task *active_task[MAX_CPUS]; // the task that each CPU is working on
pthread_mutex_t available_task_mutex;
pthread_mutex_t mlfq_levels_mutex;
pthread_mutex_t mlfq_active_mutex;
pthread_mutex_t mlfq_done_mutex;
pthread_mutex_t active_task_mutex;
pthread_cond_t no_available_task_cond;
pthread_cond_t available_task_cond;
task *available_task;        // next task to be taken by a CPU

int main(int argc, char **argv) {
    if (argc < 4) {
        printf("Usage: ./scheduler <cpus> <s> <taskfilename>\n");
        return EXIT_FAILURE;
    }

    num_cpus = atoi(argv[1]);
    int s = atoi(argv[2]);
    filename = argv[3];

    mlfq_init(s);
    // load in the tasks from the input file
    pthread_t reader_thread;
    pthread_create(&reader_thread, NULL, reader, NULL);

    // these should run together
    pthread_t scheduler_thread;
    pthread_t worker_thread;

    pthread_create(&scheduler_thread, NULL, scheduler, NULL);
    pthread_create(&worker_thread, NULL, worker, NULL);

    pthread_join(reader_thread, NULL);
    pthread_join(worker_thread, NULL);
    pthread_join(scheduler_thread, NULL);

    report_statistics();

    printf("mlfq exiting cleanly\n");
}

#define MAX_INPUT_SIZE 1000

void mlfq_init(int s) {
    pthread_mutex_lock(&mlfq_levels_mutex);
    mlfq = malloc(sizeof(struct MLFQ));
    mlfq->s = s;
    mlfq->num_active = 0;
    mlfq->num_done = 0;
    for (int level = 0; level < MLFQ_TOP_PRIORITY; level++) {
        mlfq->num_tasks[level] = 0;
    }

    clock_gettime(CLOCK_REALTIME, &mlfq->last_reset_time);
    pthread_mutex_unlock(&mlfq_levels_mutex);
}

/**
 * Introduces a new task to the MLFQ by adding it to the array of active tasks,
 * and inserting it into the MLFQ.
 */
void mlfq_introduce(task *t) {
    verify_task(t);
    printf("XXX: Locking mlfq_active_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&mlfq_active_mutex);
    mlfq->active[mlfq->num_active++] = t;
    pthread_mutex_unlock(&mlfq_active_mutex);
    printf("XXX: Unlocked mlfq_active_mutex\n");
    fflush(stdout);
    printf("READER: Introduced task %s\n", t->task_name);

    mlfq_insert(t);
}

/**
 * Inserts the given task at the level described by its priority field.
 */
void mlfq_insert(task *t) {
    printf("XXX: Locking mlfq_levels_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&mlfq_levels_mutex);
    verify_task(t);
    mlfq->levels[t->priority][mlfq->num_tasks[t->priority]++] = t;
    pthread_mutex_unlock(&mlfq_levels_mutex);
    printf("XXX: Unlocked mlfq_levels_mutex\n");
    fflush(stdout);
}

void remove_task(task *t, task *task_arr[MAX_TASKS], int *num_tasks, pthread_mutex_t *mutex) {
    printf("XXX: Locking mutex in remove\n");
    fflush(stdout);
    pthread_mutex_lock(mutex);
    printf("XXX: acquired lock in remove\n");
    fflush(stdout);
    // TODO test
    int j = 0;
    for (int i = 0; i < *num_tasks; i++) {
        if (task_arr[i] == t) {
            assert(j == 0);
            j = 1;
        } else {
            task_arr[i - j] = task_arr[i];
        }
    }

    if (j == 1)
        (*num_tasks)--;

    pthread_mutex_unlock(mutex);
    printf("XXX: Unlocked mutex in remove\n");
    fflush(stdout);
}

/**
 * Remove and return the task at the start of the array
 */
task *pop_task(task *task_arr[MAX_TASKS], int *num_tasks, pthread_mutex_t *mutex) {
    printf("XXX: Locking mutex in pop_task\n");
    fflush(stdout);
    pthread_mutex_lock(mutex);
    if (*num_tasks == 0)
        return NULL;

    task *t = task_arr[0];
    for (int i = 0; i < (*num_tasks) - 1; i++) {
        assert(task_arr[i + 1] != NULL);
        task_arr[i] = task_arr[i + 1];
    }

    (*num_tasks)--;

    pthread_mutex_unlock(mutex);
    printf("XXX: Unlocked mutex in pop_task\n");
    fflush(stdout);
    return t;
}

void append_task(task *task_arr[MAX_TASKS], task *t, int *num_tasks, pthread_mutex_t *mutex) {
    printf("XXX: Locking mutex in append_task\n");
    fflush(stdout);
    pthread_mutex_lock(mutex);
    task_arr[(*num_tasks)++] = t;
    pthread_mutex_unlock(mutex);
    printf("XXX: Locking mutex in append_task\n");
    fflush(stdout);
}

/**
 * Returns the next avaiable task to be scheduled that has not already been scheduled.
 */
task *mlfq_next() {
    // TODO: do we need locks for this?
    for (int level = MLFQ_TOP_PRIORITY - 1; level >= 0; level--) {
        if (mlfq->num_tasks[level] > 0) {
            for (int i = 0; i < mlfq->num_tasks[level]; i++) {
                if (!mlfq->levels[level][i]->active) {
                    mlfq->levels[level][i]->active = true;
                    return mlfq->levels[level][i];
                }
            }
        }
    }

    return NULL;
}

void mlfq_done(task *t) {
    clock_gettime(CLOCK_REALTIME, &t->completion_time);
    printf("Task %s is done\n", t->task_name);

    remove_task(t, mlfq->levels[t->priority], &mlfq->num_tasks[t->priority], &mlfq_levels_mutex);
    remove_task(t, mlfq->active, &mlfq->num_active, &mlfq_active_mutex);
    append_task(mlfq->done, t, &mlfq->num_done, &mlfq_done_mutex);
}

bool mlfq_is_empty() {
    printf("XXX: Locking mlfq_levels_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&mlfq_levels_mutex);
    bool is_empty = true;
    for (int i = 0; i < MLFQ_TOP_PRIORITY; i++) {
        if (mlfq->num_tasks[i] > 0) {
            is_empty = false;
        }
    }

    pthread_mutex_unlock(&mlfq_levels_mutex);
    printf("XXX: Unlocked mlfq_levels_mutex\n");
    fflush(stdout);
    return is_empty;
}

/**
 * MLFQ is done if the following is true:
 * 1. There are no tasks in the MLFQ
 * 2. There are no tasks currently executing on a CPU
 * 3. There are no tasks waiting to be executed on a CPU
 */
bool mlfq_is_done() {
    bool done = true;
    // If any of the MLFQ levels are non-empty, we're not done
    if (!mlfq_is_empty()) {
        done = false;
    }

    printf("XXX: Locking available_task_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&available_task_mutex);
    // If we aren't done reading, or there's still an available task, we know we're not done
    if (!reading_done || available_task != NULL) {
        done = false;
    }

    pthread_mutex_unlock(&available_task_mutex);
    printf("XXX: Unlocked available_task_mutex\n");
    fflush(stdout);

    // If any CPUs are currently working on a task, we're not done
    printf("XXX: Locking active_task_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&active_task_mutex);
    if (mlfq->num_active > 0) {
        done = false;
    }

    pthread_mutex_unlock(&active_task_mutex);
    printf("XXX: Unlocked active_task_mutex\n");
    fflush(stdout);
    return done;
}

/**
 * Remove the task from it's previous priority level, and insert it
 * at a new priority level.
 */
void mlfq_reintroduce(task *t, int new_priority) {
    //printf("reintroducing task %s\n", t->task_name);
    printf("XXX: Locking %s mutex\n", t->task_name);
    fflush(stdout);
    pthread_mutex_lock(&t->mutex);
    // remove the task from it's previous priority level
    remove_task(t, mlfq->levels[t->priority], &mlfq->num_tasks[t->priority], &mlfq_levels_mutex);
    t->priority = new_priority;
    t->active = false;
    // insert the task at it's new priority level
    mlfq_insert(t);
    printf("XXX: Unlocked %s mutex\n", t->task_name);
    fflush(stdout);
    pthread_mutex_unlock(&t->mutex);
}

/**
 * Reset the priority of all tasks in the MLFQ to be top priority.
 */
void mlfq_reset_queues() {
    printf("XXX: Locking mlfq_active_mutex\n");
    fflush(stdout);
    pthread_mutex_lock(&mlfq_active_mutex);
    printf("Resetting all queues\n");

    if (mlfq->num_active > 0) {
        // Reset the priority of all active tasks
        for (int level = 0; level < MLFQ_TOP_PRIORITY; level++) {
            for (int i = 0; i < mlfq->num_tasks[level]; i++) {
                task *t = mlfq->levels[level][i];
                mlfq_reintroduce(t, MLFQ_TOP_PRIORITY - 1);
            }
        }
    }

    struct timespec current_time;
    clock_gettime(CLOCK_REALTIME, &current_time);
    mlfq->last_reset_time = current_time;

    pthread_mutex_unlock(&mlfq_active_mutex);
    printf("XXX: Unlocked mlfq_active_mutex\n");
    fflush(stdout);
}

#define DELAY_TOKEN "DELAY"

/**
 * Asynchronously read tasks from an input file and introduce them to the MLFQ scheduler.
 */
void *reader(void *args) {
    (void) args;
    int ms_delay, task_type, task_length, odds_of_io;
    task *new_task;
    char input[MAX_INPUT_SIZE];
    char *task_name, *dup_task_name;
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file %s\n", filename);
        exit(1);
    }

    // Read all lines in the input file
    while (fgets(input, MAX_INPUT_SIZE, file) != NULL) {
        task_name = strtok(input, " ");
        if (strcmp(task_name, DELAY_TOKEN) == 0) {
            ms_delay = atoi(strtok(NULL, " "));
            microsleep(ms_delay * 1000);
        } else {
            // Create a new task from the data on the input line and add it to the scheduler
            dup_task_name = malloc(sizeof(char) * strlen(task_name));
            strcpy(dup_task_name, task_name);
            task_type = atoi(strtok(NULL, " "));
            task_length = atoi(strtok(NULL, " "));
            odds_of_io = atoi(strtok(NULL, " "));

            new_task = malloc(sizeof(task));
            new_task->task_name = dup_task_name;
            new_task->task_type = task_type;
            new_task->task_length = task_length;
            new_task->time_remaining = task_length;
            new_task->priority = MLFQ_TOP_PRIORITY - 1;
            new_task->odds_of_io = odds_of_io;
            new_task->active = false;
            clock_gettime(CLOCK_REALTIME, &new_task->arrival_time);
            verify_task(new_task);
            mlfq_introduce(new_task);
        }
    }

    printf("READER: exiting\n");
    reading_done = true;
    return NULL;
}

void *scheduler(void *args) {
    (void) args;
    struct timespec current_time;

    printf("SCHEDULER: starting\n");
    while (!mlfq_is_done()) {
        printf("XXX: Locked available_task_mutex\n");
        fflush(stdout);
        pthread_mutex_lock(&available_task_mutex);
        // Wait until our previously scheduled task was taken by a CPU
        while (available_task != NULL) {
            pthread_cond_wait(&no_available_task_cond, &available_task_mutex);
        }

        // Rule 5: After some time period S, move all the tasks in the system to the topmost queue
        clock_gettime(CLOCK_REALTIME, &current_time);
        if (timespec_to_usecs(current_time) - timespec_to_usecs(mlfq->last_reset_time) >= mlfq->s) {
            mlfq_reset_queues();
        }

        // Set the next available task to be executed
        available_task = mlfq_next();
        if (available_task != NULL) {
            verify_task(available_task);
            available_task->active = true;
            pthread_cond_signal(&available_task_cond);
        }

        printf("XXX: Unlocked available_task_mutex\n");
        fflush(stdout);
        pthread_mutex_unlock(&available_task_mutex);
    }

    printf("SCHEDULER: exiting\n");
    return NULL;
}

void *worker(void *args) {
    (void) args;

    printf("WORKER: starting\n");
    while (!mlfq_is_done()) {
        // Get a task to work on
        printf("XXX: Locked available_task_mutex\n");
        fflush(stdout);
        pthread_mutex_lock(&available_task_mutex);
        printf("WORKER: waiting for a task\n");
        while (available_task == NULL) {
            pthread_cond_wait(&available_task_cond, &available_task_mutex);
        }

        assert(active_task[0] == NULL);
        active_task[0] = available_task;
        verify_task(active_task[0]);
        available_task = NULL; // we've taken responsibility for this task, no other worker can now take it
        pthread_cond_signal(&no_available_task_cond);
        pthread_mutex_unlock(&available_task_mutex);
        printf("XXX: Unlocked available_task_mutex\n");
        fflush(stdout);

        // Update first CPU time if it's our first time running the task
        if (!active_task[0]->ran_before) {
            active_task[0]->ran_before = true;
            clock_gettime(CLOCK_REALTIME, &active_task[0]->first_cpu_time);
        }

        print_task(active_task[0]);

        // Determine how much time this task is going to take
        int sleep_time; // TODO: Race condition
        if (active_task[0]->task_type == io_task) { // TODO: Don't only do for IO tasks
            int rand_io = rand() % 100;
            if (active_task[0]->odds_of_io < rand_io) {
                sleep_time = rand() % QUANTUM_LEN;
                if (sleep_time > active_task[0]->time_remaining) {
                    sleep_time = active_task[0]->time_remaining;
                }
            } else {
                sleep_time = QUANTUM_LEN;
            }
        } else {
            // Sleep until the task is done or time quantum is complete
            if (active_task[0]->time_remaining > QUANTUM_LEN) {
                sleep_time = QUANTUM_LEN;
            } else {
                sleep_time = active_task[0]->time_remaining;
            }
        }

        // Sleep for alloted time
        microsleep(sleep_time);
        printf("WORKER: Finished %s in %u\n", active_task[0]->task_name, sleep_time);

        active_task[0]->time_remaining -= sleep_time;
        assert(active_task[0]->time_remaining >= 0);
        int new_priority = active_task[0]->priority;
        // Drop priority if entire quantum is used
        if (sleep_time == QUANTUM_LEN && active_task[0]->priority > 0) { // TODO: Track total time, comp. TIME_ALLOTMENT
            new_priority = active_task[0]->priority - 1;
        }

        if (active_task[0]->time_remaining > 0) {
            // We're not done the task, send it back to the scheduler
            mlfq_reintroduce(active_task[0], new_priority);
            verify_task(active_task[0]);
        } else if (active_task[0]->time_remaining == 0) {
            mlfq_done(active_task[0]);
        }

        active_task[0] = NULL;

    }

    printf("WORKER: exiting\n");
    return NULL;
}

struct timespec turnaround_time(task *t) {
    return diff(t->arrival_time, t->completion_time);
}

struct timespec response_time(task *t) {
    return diff(t->arrival_time, t->first_cpu_time);
}

void report_statistics() {
    task *done_task;
    long average_turnaround[NUM_TASK_TYPES] = {0},
            average_response[NUM_TASK_TYPES] = {0};
    printf("%lu\n", average_turnaround[1]);
    int t = 1;
    while (mlfq->num_done > 0) {
        done_task = pop_task(mlfq->done, &mlfq->num_done, &mlfq_done_mutex);

        average_turnaround[done_task->task_type] += (timespec_to_usecs(turnaround_time(done_task)) - average_turnaround[done_task->task_type]) / t;
        average_response[done_task->task_type] += (timespec_to_usecs(response_time(done_task)) - average_response[done_task->task_type]) / t;

        ++t;
    }

    printf("Average turnaround time per type:\n");
    for (int i = 0; i < NUM_TASK_TYPES; i++) {
        printf("Type %d: %lu usec\n", i, average_turnaround[i]);
    }
    printf("Average response time per type:\n");
    for (int i = 0; i < NUM_TASK_TYPES; i++) {
        printf("Type %d: %lu usec\n", i, average_response[i]);
    }
}

void print_task(task *t) {
    printf("working on task:\n"
           "\ttask name: %s\n"
           "\ttime remaining: %u\n"
           "\tpriority: %d\n",
           t->task_name,
           t->time_remaining,
           t->priority);
}

void verify_task(task *t) {
    struct timespec curr_time;
    clock_gettime(CLOCK_REALTIME, &curr_time);
    // assert(t->time_remaining > 0);
    assert(t->priority < MLFQ_TOP_PRIORITY && t->priority >= 0);
    //assert(timespec_to_usecs(t->arrival_time) < timespec_to_usecs(curr_time));
    //assert(timespec_to_usecs(t->first_cpu_time) < timespec_to_usecs(curr_time));
    //assert(timespec_to_usecs(t->completion_time) < timespec_to_usecs(curr_time));
}

#define NANOS_PER_USEC 1000
#define USEC_PER_SEC 1000000

static void microsleep(unsigned int usecs) {
    long seconds = usecs / USEC_PER_SEC;
    long nanos = (usecs % USEC_PER_SEC) * NANOS_PER_USEC;
    struct timespec t = {.tv_sec = seconds, .tv_nsec = nanos};
    int ret;
    do {
        ret = nanosleep(&t, &t);
    } while (ret == -1 && (t.tv_sec || t.tv_nsec));
}

struct timespec diff(struct timespec start, struct timespec end) {
    struct timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}

long timespec_to_usecs(struct timespec ts) {
    return ts.tv_sec * USEC_PER_SEC + ts.tv_nsec / NANOS_PER_USEC;
}
