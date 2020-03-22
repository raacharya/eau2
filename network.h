#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <iostream>
//#include "store.h"
#include "serial.h"
#include "efficientArray.h"
#include <thread>
#include <map>

#define MAXDATASIZE 1024
#define SERVERIP "127.0.0.1"
#define SERVERPORT "9034"

using namespace std;

unsigned short* get_in_port(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_port);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_port);
}

void add_to_pfds(struct pollfd *pfds[], int newfd, int *fd_count)
{
    (*pfds)[*fd_count].fd = newfd;
    (*pfds)[*fd_count].events = POLLIN; // Check ready-to-read

    (*fd_count)++;
}

void del_from_pfds(struct pollfd pfds[], int i, int *fd_count)
{
    pfds[i] = pfds[*fd_count-1];

    (*fd_count)--;
}

void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int get_listener_socket(char* ip, char* port) {
    int listener;
    int yes=1;
    int rv;

    struct addrinfo hints, *ai, *p;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if ((rv = getaddrinfo(ip, port, &hints, &ai)) != 0) {
        fprintf(stderr, "error getting address information: %s\n", gai_strerror(rv));
        exit(1);
    }

    for(p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener < 0) {
            continue;
        }

        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        if (bind(listener, p->ai_addr, p->ai_addrlen) < 0) {
            close(listener);
            continue;
        }

        break;
    }

    if (p == NULL) {
        return -1;
    }

    freeaddrinfo(ai);

    if (listen(listener, 10) == -1) {
        return -1;
    }

    return listener;
}

int connect_to_server(char* ip, char* port) {
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];
    int server_sock;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if ((rv = getaddrinfo(ip, port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((server_sock = socket(p->ai_family, p->ai_socktype,
                             p->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }

        if (connect(server_sock, p->ai_addr, p->ai_addrlen) == -1) {
            close(server_sock);
            perror("client: connect");
            continue;
        }

        break;
    }

    if (p == NULL) {
        fprintf(stderr, "client: failed to connect\n");
        return -1;
    }

    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
              s, sizeof s);
    printf("client: connecting to %s\n", s);
    return server_sock;
}



class ClientSocket {
    public:
        char* ip;
        char* port;

        int listener, server_sock, numbytes;
        char buf[MAXDATASIZE];

        int newfd;
        struct sockaddr_storage remoteaddr;
        socklen_t addrlen;
        char remoteIP[INET6_ADDRSTRLEN];
        std::thread t1;
        bool alive = true;

        int fd_count = 0;
        int fd_size = 6;
        struct pollfd *pfds = new pollfd[fd_size];
        FixedStrArray* directory;
        FixedIntArray* otherClients = new FixedIntArray();
        Serializer* serializer = new Serializer();

        ClientSocket(char* ip1, char* port1) {
            ip = ip1;
            port = port1;
            listener = get_listener_socket(ip, port);

            if (listener == -1) {
                fprintf(stderr, "client: error getting listening socket\n");
                exit(1);
            }
        }

        void bind_server() {
            if ((server_sock = connect_to_server(SERVERIP, SERVERPORT)) < 0) {
                fprintf(stderr, "client: error connecting to server\n");
            }
            strcpy(buf, ip);
            buf[strlen(ip)] = '\0';
            if (send(server_sock, buf, MAXDATASIZE, 0) == -1) {
                perror("send");
            }
        }

        char* read() {
            pfds[0].fd = listener;
            pfds[0].events = POLLIN; // Report ready to read on incoming connection
            pfds[1].fd = server_sock;
            pfds[1].events = POLLIN;
            fd_count = 2; // For the listener and server sockets

            while(alive) {
                int poll_count = poll(pfds, fd_count, -1);

                if (poll_count == -1) {
                    perror("poll");
                    exit(1);
                }

                for(int i = 0; i < fd_count; i++) {

                    if (pfds[i].revents & POLLIN) {

                        if (pfds[i].fd == listener) {

                            addrlen = sizeof remoteaddr;
                            newfd = accept(listener,
                                           (struct sockaddr *)&remoteaddr,
                                           &addrlen);

                            if (newfd == -1) {
                                perror("accept");
                            } else {
                                add_to_pfds(&pfds, newfd, &fd_count);

                                printf("pollclient: new connection from %s on "
                                       "socket %d on port %d\n",
                                       inet_ntop(remoteaddr.ss_family,
                                                 get_in_addr((struct sockaddr*)&remoteaddr),
                                                 remoteIP, INET6_ADDRSTRLEN),
                                       newfd, ntohs(*get_in_port((sockaddr*)&remoteaddr)));
                            }
                        } else if (pfds[i].fd == server_sock) {
                            int nbytes = recv(server_sock, buf, sizeof buf, 0);

                            if (nbytes <= 0) {
                                if (nbytes == 0) {
                                    printf("pollclient: server hung up\n");
                                } else {
                                    perror("recv");
                                }

                                close(server_sock); // Bye!
                                exit(0);

                            } else {
                                printf("%s\n", buf);
                                FixedStrArray* newDir = serializer->deserializeFixedStringArr(buf);
                                if (directory != NULL) {
                                    for (int k = 0; k < directory->size(); k += 1) {
                                        if (k == newDir->size() || !directory->get(k)->equals(newDir->get(k))) {
                                            otherClients->removeAndSwitch(k);
                                            break;
                                        }
                                    }
                                }
                                directory = newDir;
                                for (int k = 0; k < directory->size(); k += 1) {
                                    if (otherClients->used == k) {
                                        int newClient = connect_to_server(directory->get(k)->c_str(), SERVERPORT);
                                        otherClients->pushBack(newClient);
                                    }
                                }
                            }
                        } else {
                            int nbytes = recv(pfds[i].fd, buf, sizeof buf, 0);

                            int sender_fd = pfds[i].fd;

                            if (nbytes <= 0) {
                                if (nbytes == 0) {
                                    printf("pollclient: socket %d hung up\n", sender_fd);
                                } else {
                                    perror("recv");
                                }

                                close(pfds[i].fd); // Bye!

                                del_from_pfds(pfds, i, &fd_count);

                            } else {
                                printf("client: received '%s'\n", buf);
                            }
                        }
                    }
                }
            }
        }

        void sendMessages() {
            t1 = std::thread(&ClientSocket::handleUserInput, this);
        }

        void handleUserInput() {
            for(;;) {
                char line[MAXDATASIZE];
                fgets(line, sizeof line, stdin);
                if (strcmp(line, "directory\n") == 0) {
                    for (int i = 0; i < directory->size(); i += 1) {
                        printf("%d: %s\n", i, directory->get(i)->c_str());
                    }
                } else if (strcmp(line, "quit\n") == 0) {
                    alive = false;
                    break;
                } else if (strncmp(line, "send", 4) == 0) {
                    char dest[3];
                    int i;
                    for (i = 5; i < 8; i += 1) {
                        if (line[i] == ' ') {
                            dest[i - 5] = '\0';
                            break;
                        };
                        dest[i - 5] = line[i];
                    }
                    char message[MAXDATASIZE];
                    int messageIndex = 0;
                    i += 1;
                    for (i; i < MAXDATASIZE; i += 1) {
                        if (line[i] == '\n') {
                            message[messageIndex] = '\0';
                            break;
                        }
                        message[messageIndex] = line[i];
                        messageIndex += 1;
                    }
                    int index = atoi(dest);
                    if (index < directory->size()) {
                        if (send(otherClients->get(index), message, MAXDATASIZE, 0) == -1) {
                            perror("send");
                        }
                    } else {
                        printf("Not a valid destination\n");
                    }
                }
            }
        }

        void closeSocket() {
            t1.join();
            for (int i = 0; i < fd_count; i += 1) {
                close(pfds[i].fd);
            }
        }

};

class Key : public Object {
    public:
        String* key;
        int node;

        Key(String* strKey, int homeNode) : Object() {
            key = strKey;
            node = homeNode;
        }

        bool equals(Object* o) {
            if (o == this) return true;
            Key* other = dynamic_cast<Key*>(o);
            if (other == nullptr) return false;
            return key->equals(other->key);
        }

        size_t hash_me() {
            return key->hash_me();
        }
};

class Distributable : public Object {
    public:
        map<Key, Object*> kvStore;

        void sendToNode(Key key, Object value) {
            // send the given key value store to the given node
            // the node is the node of key
        }

        Object getFromNode(Key key) {
            Object* val = kvStore.find(key)->second;
            if (val == nullptr) {
                // send a request to the other nodes
            }
        }
};

/*************************************************************************
 * DistEffStrArr::
 * Holds String values in distributed nodes. Is unmodifiable.
 */
class DistEffStrArr : public Distributable {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        FixedStrArray** chunks; // not needed when we implement networking
        char* id;

        DistEffStrArr(char* id_var) {
            id = id_var;
            chunkSize = 50; // getFromNode(id-chunkSize)
            capacity = 1; // getFromNode(id-capacity)
            currentChunkIdx = 0; // getFromNode(id-currentChunkSize)
            numberOfElements = 0; // getFromNode(id-numberOfElements)

            // in the future with networking, this will not exist
            chunks = new FixedStrArray*[capacity];

            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedStrArray(chunkSize);
                // in the future, call super's sendToNode method
            }
        }

        bool equals(Object* other) {
            DistEffStrArr* o = dynamic_cast<DistEffStrArr *> (other);
            if(o) {
                for(size_t i = 0; i < numberOfElements; i++) {
                    if(!get(i)->equals(o->get(i))) {
                        return false;
                    }
                }
            }
            return numberOfElements == o->numberOfElements;
        }

        DistEffStrArr(EffStrArr& from, char* id_var) {
            // this will basically send all the chunks to the various nodes
            // as well as the metadata, essentially storing the data of this
            // in the nodes
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedStrArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedStrArray(*from.chunks[i]);
            }
            id = id_var;
        }

        String* get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            // in the future, call super's getFromNode to grab the chunk
            // and then cast to a FixedStrArr
            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        size_t size() {
            return numberOfElements;
        }

        int indexOf(String* item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        ~DistEffStrArr() {
            for (int i = 0; i < capacity; i += 1) {
                delete chunks[i];
            }
            delete[] chunks;
        }
};

/*************************************************************************
 * DistEffCharArr::
 * Holds Char values in distributed nodes. Is unmodifiable.
 */
class DistEffCharArr : public Distributable {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        FixedCharArray** chunks; // not needed when we implement networking
        char* id;

        DistEffCharArr(char* id_var) {
            id = id_var;
            chunkSize = 50; // getFromNode(id-chunkSize)
            capacity = 1; // getFromNode(id-capacity)
            currentChunkIdx = 0; // getFromNode(id-currentChunkSize)
            numberOfElements = 0; // getFromNode(id-numberOfElements)

            // in the future with networking, this will not exist
            chunks = new FixedCharArray*[capacity];

            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedCharArray(chunkSize);
                // in the future, call super's sendToNode method
            }
        }

        bool equals(Object* other) {
            DistEffCharArr* o = dynamic_cast<DistEffCharArr *> (other);
            if(o) {
                for(size_t i = 0; i < numberOfElements; i++) {
                    if(get(i) != o->get(i)) {
                        return false;
                    }
                }
            }
            return numberOfElements == o->numberOfElements;
        }

        DistEffCharArr(EffCharArr& from, char* id_var) {
            // this will basically send all the chunks to the various nodes
            // as well as the metadata, essentially storing the data of this
            // in the nodes
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedCharArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedCharArray(*from.chunks[i]);
            }
        }

        char get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            // in the future, call super's getFromNode to grab the chunk
            // and then cast to a FixedStrArr
            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        size_t size() {
            return numberOfElements;
        }

        ~DistEffCharArr() {
            for (int i = 0; i < capacity; i += 1) {
                delete chunks[i];
            }
            delete[] chunks;
        }
};