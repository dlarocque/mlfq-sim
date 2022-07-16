#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

enum cpu_task_type {
    short_task,
    medium_task,
    long_task, 
    io_task
};

typedef struct Task {
    char *task_name;
    enum cpu_task_type task_type;
    unsigned int task_length;
    unsigned int odds_of_io;
} task;

void mlfq_init(char *, int, int);
bool job_queue_empty();
void round_robin(); // execute two jobs on a CPU in round robin

int main(int argc, char **argv) {
    int num_cpus, s;
    char *filename;
    if (argc < 4) {
        printf("Usage: ./scheduler <cpus> <s> <taskfilename>\n");
        return EXIT_FAILURE;
    }

    num_cpus = atoi(argv[1]);
    s = atoi(argv[2]);
    filename = argv[3];

    // load in the tasks from the input file
    mlfq_init(filename, num_cpus, s); 
    
    // initialize the MLFQ
    // add the initial tasks to the queue
}

#define MAX_INPUT_SIZE 1000
#define JOBS_LINE_NUM_TOKENS 4
void mlfq_init(char *filename, int num_cpus, int s) {
    // task_name task_type task_length odds_of_IO

    int fd = open(filename, O_RDONLY);
    if (fd <= 0) {
        printf("Failed to open file, exiting\n");
        exit(1);
    }
    char input[MAX_INPUT_SIZE];
    task new_task;
    while (read(fd, input, MAX_INPUT_SIZE) > 0) {
        new_task.task_name = strtok(input, " ");
        new_task.task_type = atoi(strtok(NULL, " "));
        new_task.task_length = atoi(strtok(NULL, " "));
        new_task.odds_of_io = atoi(strtok(NULL, " "));
    }
}
