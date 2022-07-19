#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>

enum cpu_task_type { short_task, medium_task, long_task, io_task };

#define MLFQ_TOP_PRIORITY 3
#define QUANTUM_LEN 50
#define MAX_TIME_ALLOTMENT 200
struct MLFQ {
    unsigned int s;
    TAILQ_HEAD(task_queue, Task) levels[MLFQ_TOP_PRIORITY];
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
    TAILQ_ENTRY(Task) stailq;
} task;

static void microsleep(unsigned int);
void mlfq_init(int);
void mlfq_insert_task(task *);
bool mlfq_done();
void *reader(void *);
void *scheduler(void *);
void *worker(void *);
struct timespec diff(struct timespec start, struct timespec end);
void report_statistics();

int num_cpus;
char *filename;
struct MLFQ *mlfq;
bool reading_started = false;
bool reading_done = false;
task *active_task[1]; // the task that each CPU is working on
pthread_mutex_t task_available_mutex;
pthread_mutex_t mlfq_mutex;
pthread_cond_t task_available_cond;
task *available_task; // next task to be taken by a CPU

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
    // task_name task_type task_length odds_of_IO

    pthread_mutex_lock(&mlfq_mutex);
    mlfq = malloc(sizeof(struct MLFQ));
    mlfq->s = s;

    // initialize the queues
    for (int i = 0; i < MLFQ_TOP_PRIORITY; i++) {
        TAILQ_INIT(&mlfq->levels[i]);
    }
    pthread_mutex_unlock(&mlfq_mutex);
    printf("done initializing mlfq\n");
}

void mlfq_insert_task(task *new_task) {
    pthread_mutex_unlock(&mlfq_mutex);
    assert(new_task != NULL);
    assert(new_task->time_remaining > 0);
    printf("mlfq_insert_task: task %s inserted at %d\n", new_task->task_name, new_task->priority);
    TAILQ_INSERT_TAIL(&mlfq->levels[new_task->priority], new_task, stailq);
    pthread_mutex_unlock(&mlfq_mutex);
}

/** 
 * MLFQ is done if the following is true:
 * 1. There are no tasks in the MLFQ
 * 2. There are no tasks currently executing on a CPU
 * 3. There are no tasks waiting to be executed on a CPU
 */
bool mlfq_done() {
    bool done = true;
    // acquiring this lock means that there is no CPU trying to get a hold of the 
    // task, and the MLFQ is not currently assigning a task to be executed.
    pthread_mutex_lock(&task_available_mutex);
    pthread_mutex_lock(&mlfq_mutex);

    if (reading_done && available_task == NULL) {
        // check if all levels of the MFLQ are empty 
        for (int level = MLFQ_TOP_PRIORITY-1; level >= 0; --level) {
            if (!TAILQ_EMPTY(&mlfq->levels[level])) {
                // printf("mlfq_done: level was non-empty\n");
                done = false;
                break;
            }
        }

        // check if there are any currently active CPUs
        for (int i = 0; i < 1; i++) {
            if (active_task[i] != NULL) {
                done = false;
                break;
            }
        }
    } else {
        done = false;
    }

    // printf("mlfq_done: done: %d\n", done);
    pthread_mutex_unlock(&mlfq_mutex);
    pthread_mutex_unlock(&task_available_mutex);
    return done;
}

#define DELAY_TOKEN "DELAY"
void *reader(void *args) {
    (void)args;
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file %s", filename);
        exit(1);
    }

    int ms_delay;
    unsigned int task_type, task_length, odds_of_io;
    task *new_task;
    char input[MAX_INPUT_SIZE];
    reading_started = true;
    while (fgets(input, MAX_INPUT_SIZE, file) != NULL) {
        printf("reader: read new line\n");
        char *task_name = strtok(input,  " ");
        if (strcmp(task_name, DELAY_TOKEN) == 0) {
            ms_delay = atoi(strtok(NULL, " "));
            microsleep(ms_delay * 1000);
        } else {
            char *dup_task_name = malloc(sizeof(char) * strlen(task_name));
            strcpy(dup_task_name, task_name);
            task_type = atoi(strtok(NULL, " "));
            task_length = atoi(strtok(NULL, " "));
            odds_of_io = atoi(strtok(NULL, " "));

            new_task = malloc(sizeof(task));
            new_task->task_name = dup_task_name;
            new_task->task_type = task_type;
            new_task->task_length = task_length;
            new_task->time_remaining = task_length;
            new_task->priority = MLFQ_TOP_PRIORITY-1;
            new_task->odds_of_io = odds_of_io;
            mlfq_insert_task(new_task);
            clock_gettime(CLOCK_REALTIME, &new_task->arrival_time);
        }
    }

    printf("reader: exiting\n");
    reading_done = true;
    return NULL;
}

void *scheduler(void *args) {
    (void) args;

    // temp
    while (!reading_started) {
        // printf("spin");
        ;
    }

    printf("scheduler: starting\n");
    while (!mlfq_done()) {
        pthread_mutex_lock(&task_available_mutex);
        // printf("scheduler: waiting for task\n");
        while (available_task != NULL) {
            pthread_cond_wait(&task_available_cond, &task_available_mutex);
        }

        // determine which task should be scheduled
        // printf("scheduler looking for next task\n");
        int level = MLFQ_TOP_PRIORITY-1;
        pthread_mutex_lock(&mlfq_mutex);
        while (level >= 0) {
            if (!TAILQ_EMPTY(&mlfq->levels[level])) {
                printf("SCHEDULING\n");
                available_task = TAILQ_FIRST(&mlfq->levels[level]);

                // printf("scheduler: scheduled task: %s\n", available_task->task_name);
                TAILQ_REMOVE(&mlfq->levels[level], available_task, stailq);
                
                pthread_cond_signal(&task_available_cond);
                break;

                // round-robin
                /*
                if (!TAILQ_EMPTY(&mlfq->levels[level])) {
                    task *top_task = TAILQ_FIRST(&mlfq->levels[level]);
                    printf("task b: %s\n", top_task->task_name);
                    TAILQ_REMOVE(&mlfq->levels[level], top_task, stailq);
                } else {
                }
                */
            } else {
                // printf("scheduler: did not find tasks at level %d\n", level);
                level--;
            }
        }

        pthread_mutex_unlock(&mlfq_mutex);
        pthread_mutex_unlock(&task_available_mutex);
    }

    printf("scheduler: exiting\n");
    return NULL;
}

void *worker(void *args) {
    (void)args;

    while (!mlfq_done()) {
        pthread_mutex_lock(&task_available_mutex);
        printf("worker: waiting for a task\n");
        while (available_task == NULL) {
            pthread_cond_wait(&task_available_cond, &task_available_mutex);
        }

        active_task[0] = available_task;
        available_task = NULL; // we've taken responsibility for this task, no other worker can now take it
        pthread_cond_signal(&task_available_cond);
        pthread_mutex_unlock(&task_available_mutex);

        // first time running a task, collect statistics
        if (!active_task[0]->ran_before) {
            active_task[0]->ran_before = true;
            clock_gettime(CLOCK_REALTIME, &active_task[0]->first_cpu_time);
        }

        assert(active_task[0]->time_remaining > 0);
        printf("worker: working on task:\n\ttask name: %s\n\ttime remaining: %u\n\tpriority: %d\n", active_task[0]->task_name, active_task[0]->time_remaining, active_task[0]->priority);

        int sleep_time = 0;
        if (active_task[0]->task_type == io_task) {
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
            // sleep until task is done or time quantum is complete
            if (active_task[0]->time_remaining > QUANTUM_LEN) {
                sleep_time = QUANTUM_LEN;
            } else {
                sleep_time = active_task[0]->time_remaining;
            }
        }

        printf("worker: sleeping for %u microseconds\n", sleep_time);
        microsleep(sleep_time);
        printf("worker: done task");

        int prev_time_remaining = active_task[0]->time_remaining;
        assert(sleep_time >= 0);
        active_task[0]->time_remaining = active_task[0]->time_remaining - sleep_time; 
        assert(prev_time_remaining >= active_task[0]->time_remaining);

        // drop priority if entire quantum is used
        if (sleep_time == QUANTUM_LEN && active_task[0]->priority > 0) {
            printf("worker: task %s is dropping priority\n", active_task[0]->task_name);
            active_task[0]->priority--;
        }

        // put the task back in the scheduler if it's not yet complete
        if (active_task[0]->time_remaining > 0) {
            printf("worker: task %s is going back to scheduler\n", active_task[0]->task_name);
            mlfq_insert_task(active_task[0]);
        } else {
            // task is done
            clock_gettime(CLOCK_REALTIME, &active_task[0]->completion_time);
            printf("worker: task %s is done\n", active_task[0]->task_name);
        }

        active_task[0] = NULL;
    }

    printf("worker: exiting\n");
    return NULL;
}

void report_statistics() {
    double average_turnaround_time, average_response_time;
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
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}
