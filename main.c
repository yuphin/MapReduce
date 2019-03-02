#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <wait.h>
#include <memory.h>
#include <fcntl.h>
#include <errno.h>

#define MIN_NUM_ARG 3
#define STDIN 0
#define STDOUT 1
#define STDERR 2

// For debugging
int fd_is_valid(int fd) {
    return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
}
int main(int argc, char *argv[]) {
    short isMapReduce = argc > MIN_NUM_ARG;
    int numOfRunners = atoi(argv[1]);
    char word[128];
    int pids[isMapReduce ? numOfRunners * 2 : numOfRunners];
    int counter = 0;
    //FILE *fp = fopen("input.txt", "r");
    int **fds = (int **) malloc(numOfRunners * sizeof(int *));
    int **mapperFds = NULL;
    int **reducerFds = NULL;
    if (isMapReduce) {
        mapperFds = (int **) malloc(numOfRunners * sizeof(int *));
        reducerFds = (int **) malloc(numOfRunners * sizeof(int *));
    }
    for (int i = 0; i < numOfRunners; i++) {
        fds[i] = (int *) malloc(2 * sizeof(int));
        if (isMapReduce) {
            mapperFds[i] = (int *) malloc(2 * sizeof(int));
            reducerFds[i] = (int *) malloc(2 * sizeof(int));
        }
    }

    for (int i = 0; i < numOfRunners; ++i) {
        pipe(fds[i]);
        if (isMapReduce) {
            pipe(mapperFds[i]);
            pipe(reducerFds[i]);
        }
    }
    if (isMapReduce) {
        close(reducerFds[0][0]);
        close(reducerFds[0][1]);
    }
    for (int i = 0; i < numOfRunners; i++) {
        pids[i] = fork();
        if (pids[i]) {
            close(fds[i][0]);
            if (isMapReduce) {
                close(mapperFds[i][1]);
            }
        } else {
            // Setup mappers first
            char buf[10];
            dup2(fds[i][0], STDIN);
            //printf("Is it valid: %d\n",fd_is_valid(mapperFds[i][1]));
            if (isMapReduce)
                dup2(mapperFds[i][1], STDOUT);
            for (int j = 0; j < numOfRunners; j++) {
                // Close all write end from this child and all read-ends excepts for itself which will be mapped to STDIN
                close(fds[j][1]);
                close(fds[j][0]);
                if (isMapReduce) {
                    close(mapperFds[j][0]);
                    close(mapperFds[j][1]);
                    close(reducerFds[j][0]);
                    close(reducerFds[j][1]);
                }
            }
            sprintf(buf, "%d", i);
            execl(argv[2], argv[2], buf, (char *) 0);
        }
    }
    if (isMapReduce) {
        for (int k = 0; k < numOfRunners; k++) {
            pids[numOfRunners + k] = fork();
            if (pids[numOfRunners + k]) {
                close(reducerFds[k][1]);
                close(reducerFds[k][0]);
                close(mapperFds[k][0]);
            } else {
                //Setup reducers
                char buf[10];
                dup2(mapperFds[k][0], STDIN);
                if (k != 0) {
                    dup2(reducerFds[k][0], STDERR);
                }
                if (k != numOfRunners - 1) {
                    dup2(reducerFds[k + 1][1], STDOUT);
                }
                for (int j = 0; j < numOfRunners; j++) {
                    close(reducerFds[j][1]);
                    close(reducerFds[j][0]);
                    close(mapperFds[j][0]);
                    close(fds[j][1]);
                }
                sprintf(buf, "%d", k);
                execl(argv[3], argv[3], buf, (char *) 0);
                exit(0);
            }
        }
    }

    // Read from the file, up to 128 bytes
    while (fgets(word, 128, stdin) != NULL) {
        int key = counter % numOfRunners;
        if (write(fds[key][1], word, strlen(word)) == -1) {
            printf("Write to pipe has failed\n");
        }
        counter++;
    }
    // End of file, close the file descriptors
    for (int i = 0; i < numOfRunners; i++) {
        close(fds[i][1]);
    }
    //Clean up
    for (int i = 0; i < (isMapReduce ? numOfRunners * 2 : numOfRunners); i++) {
        waitpid(pids[i], NULL, 0);
    }
    for (int i = 0; i < numOfRunners; i++) {
        free(fds[i]);
        if (isMapReduce) {
            free(mapperFds[i]);
            free(reducerFds[i]);
        }

    }
    free(fds);
    if (isMapReduce) {
        free(mapperFds);
        free(reducerFds);
    }

    return 0;
}