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
int fd_is_valid(int fd){
    return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
}
void mapper(int numOfRunners,char mapperName[]){
    char word[128];
    int pids[numOfRunners];
    int counter =0;
    //FILE*fp = fopen("input.txt","r");
    //Initialize file descriptors
    int **fds = (int **)malloc(numOfRunners * sizeof(int*));
    for(int i=0; i< numOfRunners; i++)
        fds[i] = (int*)malloc(2* sizeof(int));
    // Initiate pipes
    for (int i = 0; i < numOfRunners; ++i) {
        pipe(fds[i]);
    }
    for (int i = 0; i < numOfRunners; ++i) {
        pids[i] = fork();
        if(pids[i]){
            // Parent
            // Close read end
            close(fds[i][0]);
        }else{
            // Child
            char buf[10];
            for(int j= 0; j< numOfRunners; j++){
                // Close all write end from this child and all read-ends excepts for itself which will be mapped to STDIN
                close(fds[j][1]);
                if(j == i)
                    continue;
                else
                    close(fds[j][0]);
            }
            dup2(fds[i][0],STDIN);
            close(fds[i][0]);
            sprintf(buf,"%d",i);
            execl(mapperName,mapperName,buf,(char*)0);
        }
    }
    // Read from the file, up to 128 bytes
    while(fgets(word,128,stdin) != NULL){
        int key = counter % numOfRunners;
        if(write(fds[key][1],word, strlen(word)) == -1){
            printf("Write to pipe has failed\n");
        }
        counter++;
    }
    // End of file, close the file descriptors
    for(int i =0; i< numOfRunners; i++){
        close(fds[i][1]);
    }

    //Clean up
    for(int i=0; i< numOfRunners;i++){
        waitpid(pids[i],NULL,0);
    }
    for(int i = 0; i < numOfRunners; i++){
        free(fds[i]);
    }
    free(fds);
}
void mapReducer(int numOfRunners,char mapperName[],char reducerName[]){
    char word[128];
    int pids[numOfRunners*2];
    int counter =0;
    //FILE*fp = fopen("input.txt","r");
    int **fds = (int **)malloc(numOfRunners * sizeof(int*));
    int **mapperFds = (int **)malloc(numOfRunners * sizeof(int*));
    int **reducerFds = (int **)malloc(numOfRunners * sizeof(int*));
    for(int i=0; i< numOfRunners; i++){
        fds[i] = (int*)malloc(2* sizeof(int));
        mapperFds[i] = (int*)malloc(2* sizeof(int));
        reducerFds[i] = (int*)malloc(2* sizeof(int));
    }

    for (int i = 0; i < numOfRunners; ++i) {
        pipe(fds[i]);
        pipe(mapperFds[i]);
        pipe(reducerFds[i]);
    }
    close(reducerFds[0][0]);
    close(reducerFds[0][1]);
    for(int i =0;i < numOfRunners; i++){
        pids[i] = fork();
        if(pids[i]){
            close(fds[i][0]);
            close(mapperFds[i][1]);

        }else{
            // Setup mappers first
            char buf[10];
            dup2(fds[i][0],STDIN);
            //printf("Is it valid: %d\n",fd_is_valid(mapperFds[i][1]));
            dup2(mapperFds[i][1],STDOUT);
            for(int j= 0; j< numOfRunners; j++){
                // Close all write end from this child and all read-ends excepts for itself which will be mapped to STDIN
                close(fds[j][1]);
                close(fds[j][0]);
                close(mapperFds[j][0]);
                close(mapperFds[j][1]);
                close(reducerFds[j][0]);
                close(reducerFds[j][1]);
            }
            sprintf(buf,"%d",i);
            execl(mapperName,mapperName,buf,(char*)0);
        }
    }
    for(int k= 0; k < numOfRunners; k++){
        pids[numOfRunners +k] = fork();
        if(pids[numOfRunners +k]){
            close(reducerFds[k][1]);
            close(reducerFds[k][0]);
            close(mapperFds[k][0]);
        }else{
            //Setup reducers
            char buf[10];
            dup2(mapperFds[k][0],STDIN);
            if(k != 0){
                dup2(reducerFds[k][0],STDERR);
            }
            if(k != numOfRunners -1){
                dup2(reducerFds[k+1][1],STDOUT);
            }
            for(int j =0; j< numOfRunners; j++){
                close(reducerFds[j][1]);
                close(reducerFds[j][0]);
                close(mapperFds[j][0]);
                close(fds[j][1]);
            }
            sprintf(buf,"%d",k);
            execl(reducerName,reducerName,buf,(char*)0);
            exit(0);
        }
    }
    // Read from the file, up to 128 bytes
    while(fgets(word,128,stdin) != NULL){
        int key = counter % numOfRunners;
        if(write(fds[key][1],word, strlen(word)) == -1){
            printf("Write to pipe has failed\n");
        }
        counter++;
    }
    // End of file, close the file descriptors
    for(int i =0; i< numOfRunners; i++){
        close(fds[i][1]);
    }
    //Clean up
    for(int i=0; i< numOfRunners*2;i++){
        waitpid(pids[i],NULL,0);
    }
    for(int i = 0; i < numOfRunners; i++){
        free(fds[i]);
        free(mapperFds[i]);
        free(reducerFds[i]);
    }
    free(fds);
    free(mapperFds);
    free(reducerFds);

}
int main(int argc, char *argv[]) {
    if(argc == MIN_NUM_ARG){
        mapper(atoi(argv[1]),argv[2]);
    }else{
        mapReducer(atoi(argv[1]),argv[2],argv[3]);
    }
    return 0;
}