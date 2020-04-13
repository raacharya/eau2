
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include "network.h"
#include "../util/string.h"
#include "../array/efficientArray.h"
#include "serial.h"

#define PORT "9034"

// Main
int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr,"usage: server hostname\n");
        exit(1);
    }

    int listener;

    int newfd;
    struct sockaddr_storage remoteaddr;
    socklen_t addrlen;

    char buf[1024];

    char remoteIP[INET6_ADDRSTRLEN];

    int fd_count = 0;
    int fd_size = 5;
    struct pollfd *pfds = new pollfd[fd_size];
    FixedStrArray* directory = new FixedStrArray(fd_size);
    Serializer serializer{};

    listener = get_listener_socket(argv[2], PORT);

    if (listener == -1) {
        fprintf(stderr, "error getting listening socket\n");
        exit(1);
    }

    pfds[0].fd = listener;
    pfds[0].events = POLLIN; // Report ready to read on incoming connection

    fd_count = 1; // For the listener

    for(;;) {
        int poll_count = poll(pfds, fd_count, -1);

        if (poll_count == -1) {
            perror("poll");
            exit(1);
        }

        for(int i = 0; i < fd_count; i++) {

            if (pfds[i].revents & POLLIN) { // We got one!!

                if (pfds[i].fd == listener) {

                    addrlen = sizeof remoteaddr;
                    newfd = accept(listener,
                                   (struct sockaddr *)&remoteaddr,
                                   &addrlen);

                    if (newfd == -1) {
                        perror("accept");
                    } else {
                        add_to_pfds(&pfds, newfd, &fd_count);

                        printf("pollserver: new connection from %s on "
                               "socket %d on port %d\n",
                               inet_ntop(remoteaddr.ss_family,
                                         get_in_addr((struct sockaddr*)&remoteaddr),
                                         remoteIP, INET6_ADDRSTRLEN),
                               newfd, ntohs(*get_in_port((sockaddr*)&remoteaddr)));
                    }
                } else {
                    int nbytes = recv(pfds[i].fd, buf, sizeof buf, 0);

                    int sender_fd = pfds[i].fd;

                    if (nbytes <= 0) {
                        if (nbytes == 0) {
                            printf("pollserver: socket %d hung up\n", sender_fd);
                        } else {
                            perror("recv");
                        }

                        close(pfds[i].fd); // Bye!

                        del_from_pfds(pfds, i, &fd_count);
                        directory->removeAndSwitch(i);

                    } else {
                        printf("%s\n", buf);
                        directory->pushBack(new String(buf));
                    }
                    char* serializedDir = serializer.serializeFixedStrArray(directory);
                    for(int j = 0; j < fd_count; j++) {
                        int dest_fd = pfds[j].fd;

                        if (dest_fd != listener) {
                            if (send(dest_fd, serializedDir, nbytes, 0) == -1) {
                                perror("send");
                            }
                        }
                    }
                }
            }
        }
    }

    return 0;
}