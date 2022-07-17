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

typedef struct Task {
    char *task_name;
    enum cpu_task_type task_type;
    unsigned int task_length;
    unsigned int odds_of_io;
    STAILQ_ENTRY(Task) stailq;
} task;

#define MLFQ_LEVELS 3
#define QUANTUM_LEN 50
#define MAX_TIME_ALLOTMENT 200
struct MLFQ {
    unsigned int s;
    STAILQ_HEAD(task_queue, Task) levels[MLFQ_LEVELS];
};

static void microsleep(unsigned int);
void mlfq_init(int);
void mlfq_insert_new_task(task *);
void mlfq_insert(task *, unsigned int);
void *reader(void *);
void *worker(void *);

int num_cpus;
char *filename;
struct MLFQ *mlfq;
pthread_mutex_t task_available_mutex;
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
    pthread_join(reader_thread, NULL);
}

#define MAX_INPUT_SIZE 1000
#define JOBS_LINE_NUM_TOKENS 4
void mlfq_init(int s) {
    // task_name task_type task_length odds_of_IO

    mlfq = malloc(sizeof(struct MLFQ));
    mlfq->s = s;

    // initialize the queues
    for (int i = 0; i < MLFQ_LEVELS; i++) {
        STAILQ_INIT(&mlfq->levels[i]);
    }
}

void mlfq_insert_new_task(task *new_task) {
    assert(new_task != NULL);
    mlfq_insert(new_task, MLFQ_LEVELS-1);
}

void mlfq_insert(task *new_task, unsigned int level) {
    STAILQ_INSERT_TAIL(mlfq->levels[level], new_task, new_task->task_name);
}

#define DELAY_TOKEN "DELAY"
#define SHORT_TASK_TOKEN "short_task"
#define MED_TASK_TOKEN "med_task"
#define LONG_TASK_TOKEN "long_task"
#define IO_TASK_TOKEN "io_task"
void *reader(void *args) {
    (void)args;
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file %s", filename);
        exit(1);
    }

    int ms_delay;
    char *task_name;
    unsigned int task_type, task_length, odds_of_io;
    task *new_task;
    char input[MAX_INPUT_SIZE];
    while (fgets(input, MAX_INPUT_SIZE, file) != NULL) {
        task_name = strtok(input,  " ");
        if (strcmp(task_name, DELAY_TOKEN) == 0) {
            ms_delay = atoi(strtok(NULL, " "));
            microsleep(ms_delay * 1000);
        } else {
            task_type = atoi(strtok(NULL, " "));
            task_length = atoi(strtok(NULL, " "));
            odds_of_io = atoi(strtok(NULL, " "));

            new_task = malloc(sizeof(task));
            new_task->task_name = task_name;
            new_task->task_type = task_type;
            new_task->task_length = task_length;
            new_task->odds_of_io = odds_of_io;
            mlfq_insert(new_task);
        }
    }

    return NULL;
}

void *worker(void *args) {
    (void)args;
    // task *current_task;

    pthread_mutex_lock(&task_available_mutex);
    // wait until there is a task for us to start
    while (available_task != NULL) {
        pthread_cond_wait(&task_available_cond, &task_available_mutex);
    }

    // current_task = available_task;
    available_task = NULL;
    pthread_mutex_unlock(&task_available_mutex);

    // find odds of IO, IO time
    // sleep until IO is done
    // sleep until task is done, time slice is complete, or io is done

    return NULL;
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
