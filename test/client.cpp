#include "../network/network.h"

#define PORT "9034" // the port client will be connecting to

int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr,"usage: client hostname\n");
        exit(1);
    }

    ClientSocket* sock = new ClientSocket(argv[2], PORT);

    sock->bind();

    sock->sendMessages();
    sock->read();


    sock->closeSocket();

    return 0;
}

























//#include <stdio.h>
//#include <stdlib.h>
//#include <unistd.h>
//#include <string.h>
//#include <fcntl.h>
//#include <pthread.h>
//#include <netinet/in.h>
//#include <sys/socket.h>
//#include <sys/types.h>
//#include <arpa/inet.h>
//
//void * doRecieving(void * sockID){
//
//    int clientSocket = *((int *) sockID);
//
//    while(1){
//
//        char data[1024];
//        int read = recv(clientSocket,data,1024,0);
//        data[read] = '\0';
//        printf("%s\n",data);
//
//    }
//
//}
//
//int main(){
//
//    int clientSocket = socket(PF_INET, SOCK_STREAM, 0);
//
//    struct sockaddr_in serverAddr;
//
//    serverAddr.sin_family = AF_INET;
//    serverAddr.sin_port = htons(9034);
//    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
//
//    if(connect(clientSocket, (struct sockaddr*) &serverAddr, sizeof(serverAddr)) == -1) return 0;
//
//    printf("Connection established ............\n");
//
//    pthread_t thread;
//    pthread_create(&thread, NULL, doRecieving, (void *) &clientSocket );
//
//    while(1){
//        printf("here");
//        char input[4];
//        scanf("%s",input);
//        printf("here2");
//
//        if(strcmp(input,"LIST") == 0){
//
//            send(clientSocket,input,1024,0);
//
//        }
//        if(strcmp(input,"SEND") == 0){
//
//            send(clientSocket,input,1024,0);
//
//            scanf("%s",input);
//            send(clientSocket,input,1024,0);
//
//            scanf("%[^\n]s",input);
//            send(clientSocket,input,1024,0);
//
//        }
//
//    }
//
//
//}