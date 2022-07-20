#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <time.h>
#include <unistd.h>

enum cpu_task_type {
    short_task,
    medium_task,
    long_task,
    io_task,
    NUM_TASK_TYPES
};

#define MLFQ_TOP_PRIORITY 3
#define QUANTUM_LEN 50
#define MAX_TIME_ALLOTMENT 200
struct MLFQ {
    unsigned int s;
    struct timespec last_reset_time;
    TAILQ_HEAD(task_queue, Task) levels[MLFQ_TOP_PRIORITY];
    TAILQ_HEAD(active_queue, Task) active;
    TAILQ_HEAD(done_queue, Task) done; // completed tasks
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
    TAILQ_ENTRY(Task) tailq;
} task;

static void microsleep(unsigned int);

void mlfq_init(int);

void mlfq_introduce(task *);

void mlfq_insert(task *);

task *mlfq_next();

void mlfq_remove(task *);

task *mlfq_pop();

void mlfq_done(task *done_task);

bool mlfq_is_done();

void mlfq_reset_queues();

void mlfq_insert_at(task *t, int priority);

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
task *active_task[1]; // the task that each CPU is working on
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

    // Initialize tail queues
    for (int i = 0; i < MLFQ_TOP_PRIORITY; i++) {
        TAILQ_INIT(&mlfq->levels[i]);
    }

    TAILQ_INIT(&mlfq->done);
    TAILQ_INIT(&mlfq->active);
    clock_gettime(CLOCK_REALTIME, &mlfq->last_reset_time);

    pthread_mutex_unlock(&mlfq_levels_mutex);
}

/**
 * Introduces a new task to the MLFQ by adding it to the list of active tasks,
 * and inserting it into the MLFQ.
 */
void mlfq_introduce(task *t) {
    verify_task(t);
    pthread_mutex_lock(&mlfq_active_mutex);
    TAILQ_INSERT_TAIL(&mlfq->active, t, tailq);
    pthread_mutex_unlock(&mlfq_active_mutex);

    printf("READER: Introduced task %s\n", t->task_name);

    mlfq_insert(t);
}

/**
 * Inserts the given task at the level described by its priority field.
 */
void mlfq_insert(task *t) {
    pthread_mutex_lock(&mlfq_levels_mutex);
    verify_task(t);
    TAILQ_INSERT_TAIL(&mlfq->levels[t->priority], t, tailq);
    pthread_mutex_unlock(&mlfq_levels_mutex);
}

void mlfq_done(task *t) {
    assert(t != NULL);
    assert(t->time_remaining == 0);

    clock_gettime(CLOCK_REALTIME, &t->completion_time);
    printf("Task %s is done\n", t->task_name);

    pthread_mutex_lock(&mlfq_active_mutex);
    TAILQ_REMOVE(&mlfq->active, t, tailq);
    pthread_mutex_unlock(&mlfq_active_mutex);

    pthread_mutex_lock(&mlfq_done_mutex);
    TAILQ_INSERT_TAIL(&mlfq->done, t, tailq);
    pthread_mutex_unlock(&mlfq_done_mutex);

}

/**
 * Returns and removes the next task to be scheduled by the MLFQ.
 */
task *mlfq_pop() {
    task *t = mlfq_next();
    if (t != NULL)
        mlfq_remove(t);

    return t;
}

/**
 * Removes and returns the next task to execute in the scheduler.
 * The next task to execute in the scheduler is the first task
 * in the highest level queue in the MLFQ.
 */
task *mlfq_next() {
    pthread_mutex_lock(&mlfq_levels_mutex);
    task *next_task = NULL;
    for (int level = MLFQ_TOP_PRIORITY - 1; level >= 0; level--) {
        if (!TAILQ_EMPTY(&mlfq->levels[level])) {
            next_task = TAILQ_FIRST(&mlfq->levels[level]);
            //assert(level == next_task->priority);
            // assert(next_task->time_remaining > 0);
            verify_task(next_task);
            if (level != next_task->priority) {
                printf("OOPS level %d, time %d\n\n", level, next_task->time_remaining);
                print_task(next_task);
            }
            break;
        }
    }
    pthread_mutex_unlock(&mlfq_levels_mutex);

    return next_task;
}

/**
 * Removes a given task from the MLFQ.
 * Assumes that the task exists at the priority that its priority field describes.
 */
void mlfq_remove(task *t) {
    pthread_mutex_lock(&mlfq_levels_mutex);
    TAILQ_REMOVE(&mlfq->levels[t->priority], t, tailq);
    pthread_mutex_unlock(&mlfq_levels_mutex);
}

/**
 * MLFQ is done if the following is true:
 * 1. There are no tasks in the MLFQ
 * 2. There are no tasks currently executing on a CPU
 * 3. There are no tasks waiting to be executed on a CPU
 */
bool mlfq_is_done() {
    bool done = true;
    // Acquiring this lock means that there is no CPU trying to get a hold of a task
    pthread_mutex_lock(&available_task_mutex);

    if (reading_done && available_task == NULL) {
        // If any of the MLFQ levels are non-empty, we're not done
        if (mlfq_next() != NULL) {
            done = false;
        }

        // If any CPUs are currently working on a task, we're not done
        pthread_mutex_lock(&active_task_mutex);
        for (int i = 0; i < num_cpus; i++) {
            if (active_task[i] != NULL) {
                done = false;
                break;
            }
        }
        pthread_mutex_unlock(&active_task_mutex);
    } else {
        // Since we aren't done reading, or there's still an available task, we know we're not done
        done = false;
    }

    pthread_mutex_unlock(&available_task_mutex);
    return done;
}


/**
 * Reset the priority of all tasks in the MLFQ to be top priority.
 */
void mlfq_reset_queues() {
    pthread_mutex_lock(&mlfq_active_mutex);
    pthread_mutex_lock(&mlfq_levels_mutex);
    struct timespec current_time;

    printf("Resetting all queues\n");

    if (!TAILQ_EMPTY(&mlfq->active)) {
        // Reset the priority of all active tasks
        task *t;
        TAILQ_FOREACH(t, &mlfq->active, tailq) {
            mlfq_insert_at(t, MLFQ_TOP_PRIORITY - 1);
        }
    }

    clock_gettime(CLOCK_REALTIME, &current_time);
    mlfq->last_reset_time = current_time;

    pthread_mutex_unlock(&mlfq_levels_mutex);
    pthread_mutex_unlock(&mlfq_active_mutex);
}

/**
 * Remove the task from its original level, then insert it at its new level.
 */
void mlfq_insert_at(task *t, int priority) {
    assert(priority >= 0 && priority < MLFQ_TOP_PRIORITY);
    verify_task(t);
    printf("Task %s is changing priority to %d\n", t->task_name, priority);
    mlfq_remove(t);
    t->priority = priority;
    mlfq_insert(t);
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
            clock_gettime(CLOCK_REALTIME, &new_task->arrival_time);
            mlfq_introduce(new_task);
        }
    }

    printf("READER: exiting\n");
    reading_done = true;
    return NULL;
}

void *scheduler(void *args) {
    (void) args;
    //struct timespec current_time;

    printf("SCHEDULER: starting\n");
    while (!mlfq_is_done()) {
        pthread_mutex_lock(&available_task_mutex);
        // Wait until our previously scheduled task was taken by a CPU
        while (available_task != NULL) {
            pthread_cond_wait(&no_available_task_cond, &available_task_mutex);
        }

        // Rule 5: After some time period S, move all the tasks in the system to the topmost queue
        //clock_gettime(CLOCK_REALTIME, &current_time);
        //if (timespec_to_usecs(current_time) - timespec_to_usecs(mlfq->last_reset_time) >= mlfq->s) {
        //    mlfq_reset_queues();
        //}

        // Set the next available task to be executed
        available_task = mlfq_pop();
        if (available_task != NULL) {
            pthread_cond_signal(&available_task_cond);
        }

        pthread_mutex_unlock(&available_task_mutex);
    }

    printf("SCHEDULER: exiting\n");
    return NULL;
}

void *worker(void *args) {
    (void) args;

    printf("WORKER: starting\n");
    while (!mlfq_is_done()) {
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

        // Update first CPU time if it's our first time running the task
        if (!active_task[0]->ran_before) {
            active_task[0]->ran_before = true;
            clock_gettime(CLOCK_REALTIME, &active_task[0]->first_cpu_time);
        }

        print_task(active_task[0]);

        // Determine how much time this task is going to take
        int sleep_time;
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


        pthread_mutex_lock(&mlfq_levels_mutex);

        active_task[0]->time_remaining -= sleep_time;
        assert(active_task[0]->time_remaining >= 0);
        printf("WORKER: Taking levels lock\n");
        // Drop priority if entire quantum is used
        if (sleep_time == QUANTUM_LEN && active_task[0]->priority > 0) { // TODO: Track total time, comp. TIME_ALLOTMENT
            active_task[0]->priority--;
        }

        if (active_task[0]->time_remaining > 0) {
            // We're not done the task, send it back to the scheduler
            //mlfq_insert(active_task[0]);
            TAILQ_INSERT_TAIL(&mlfq->levels[active_task[0]->priority], active_task[0], tailq);
        } else if (active_task[0]->time_remaining == 0) {
            mlfq_done(active_task[0]);
        }

        pthread_mutex_unlock(&mlfq_levels_mutex);

        printf("WORKER: Released levels lock\n");


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
    while (!TAILQ_EMPTY(&mlfq->done)) {
        done_task = TAILQ_FIRST(&mlfq->done);
        TAILQ_REMOVE(&mlfq->done, done_task, tailq);

        average_turnaround[done_task->task_type] +=
                (timespec_to_usecs(turnaround_time(done_task)) -
                 average_turnaround[done_task->task_type]) /
                t;
        average_response[done_task->task_type] +=
                (timespec_to_usecs(response_time(done_task)) -
                 average_response[done_task->task_type]) /
                t;

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
