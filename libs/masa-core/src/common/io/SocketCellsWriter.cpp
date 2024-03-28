/*******************************************************************************
 *
 * Copyright (c) 2010-2015   Edans Sandes
 *
 * This file is part of MASA-Core.
 * 
 * MASA-Core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MASA-Core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MASA-Core.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

#include "SocketCellsWriter.hpp"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include <sys/socket.h> /* for socket(), bind(), and connect() */
#include <arpa/inet.h>  /* for sockaddr_in and inet_ntoa() */
#include <errno.h>

#define DEBUG (0)

SocketCellsWriter::SocketCellsWriter(string hostname, int port, string shared_path) {
    this->close_socket_path = shared_path+"/closesocket.txt";
    this->failure_signal_path = shared_path+"/failure.txt";
    this->hostname = hostname;
    this->port = port;
    this->socketfd = -1;
    init();
}

SocketCellsWriter::~SocketCellsWriter() {
	close();
}

void SocketCellsWriter::close() {
    waitForFinishMessage();
    FILE* close_socket;
    close_socket = fopen(close_socket_path.c_str(), "wb");
    fclose(close_socket);
    fprintf(stderr, "SocketCellsWriter::close(): %d\n", socketfd);
    if (socketfd != -1) {
        ::close(socketfd);
        socketfd = -1;
    }
}

int SocketCellsWriter::isopen(int socket) {
    struct timeval waiting_time; waiting_time.tv_usec = 1;
    int bytes_in_buffer = 0, ret_select;

    fd_set write_descriptor; //stores amount of descriptors to be analyzed
    FD_ZERO(&write_descriptor); //0 bits -> 0 file descriptors
    FD_SET(socket, &write_descriptor); //sets 1 bit for the write_descriptor
    ret_select=select(socket+1, 0, &write_descriptor, 0, &waiting_time); //check write connection
    //printf("ret_select=%d\n", ret_select);
    return ret_select;
}

void SocketCellsWriter::waitForFinishMessage() {   
    /* This function waits for the next GPU to finish it's execution and send a "finished" message
    *  or for it's disconnection. Only after that the socket can be closed. The goal is to keep
    *  detecting the failure, even after this GPU finishes it's execution.
    */ 
    char verification[10]="waiting";
    int qtd_bytes, tries=3;
  
    printf("Finished sending cells!\n");
    printf("Waiting for the next GPU to finish it's execution...\n");
    while(strcmp(verification,"finished")!=0) {
        qtd_bytes = recv(socketfd, verification, 10, 0);
        while(qtd_bytes == 0 && tries > 0) { //tries 3 times, just in case that is not a disconnection, but the system just too slow
            qtd_bytes = recv(socketfd, verification, 10, 0);
            tries--;
            sleep(2);
            printf("Trying %d \n", 3-tries);
        } 
        if (tries == 0) {
            printf("~~~~Connection Lost!~~~~\n");
            printf("Sending errormessage to the Controller\n");
            ::close(socketfd);
            failureSignal();
            break;
        }
    }
    printf("GPU n+1 has finished! Deleting Connection...\n");
}

void SocketCellsWriter::failureSignal() {
    FILE* fd_failure;

    if(access(failure_signal_path.c_str(), F_OK)!=0) { //failure file was not created
        printf("Failure Signal Sent!\n");
        fd_failure = fopen(failure_signal_path.c_str(), "wb");
        //TODO: write the number of the failed GPU
        fclose(fd_failure);
    }
}

int SocketCellsWriter::write(const cell_t* buf, int len) {
    int tries=3, ret;

    /* This function was modified in order to detect a fail on the receiever GPU. In order to do this,
    *  the return of the send is verified. If it is -1 (the flag errno also raises), it means that the
    *  socket has disconnected. The problem is that the -1 return only happens after the third try.
    *  Because of this property, the function isopen checks if the connection is available to write.
    *  If isopen or ret=-1 are identified, the tries is decremented untilsend comes back to normal or
    *  the 3 tries are over.
    */

    ret = -1;
    while(ret==-1 && tries > 0) { //Checks if socket is closed for 3 times in order to check if connection is just slow
        if (isopen(socketfd)>0) {
            ret = send(socketfd, buf, len*sizeof(cell_t), MSG_NOSIGNAL);
            if(ret==-1) perror("send");
        }
        if (ret==-1) {
            tries--;
            printf("Trying %d \n", 3-tries);
            perror("isopen or send");
            sleep(2);
        }
    } 
    if (tries == 0) {
        printf("Connection Lost!\n");
        ::close(socketfd);
        failureSignal();
    }
    return ret;
}

int SocketCellsWriter::writeInt(global_score_t* score) {
    int ret = send(socketfd, score, sizeof(global_score_t), 0);
    if (ret == -1) {
        fprintf(stderr, "send: Socket error: -1\n");
        return 0;
    }
    return ret;
}


void SocketCellsWriter::init() {
    int rc;
    int servSock;                    /* Socket descriptor for server */
    int clntSock;                    /* Socket descriptor for client */
    struct sockaddr_in echoServAddr; /* Local address */
    struct sockaddr_in echoClntAddr; /* Client address */
    unsigned int clntLen;            /* Length of client address data structure */

    if (DEBUG) printf("SocketCellsWriter: create socket\n");
    /* Create socket for incoming connections */
    if ((servSock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "ERROR creating server socket; return code from socket() is %d\n", servSock);
        exit(-1);
    }

    int optval = 1;
    setsockopt(servSock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));



    /* Construct local address structure */
    memset(&echoServAddr, 0, sizeof(echoServAddr));   /* Zero out structure */
    echoServAddr.sin_family = AF_INET;                /* Internet address family */
    echoServAddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
    echoServAddr.sin_port = htons(port);      /* Local port */

    //printf ("\n\n *** port used: %d *** \n\n", port);

    /* Bind to the local address */
    if (DEBUG) printf("SocketCellsWriter: Bind to local address %d\n", port);
    if ((rc = bind(servSock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr))) < 0) {
        fprintf(stderr, "ERROR; return code from bind() yyyyyyyyyy is %d\n", rc);
        exit(-1);
    }

    /* Mark the socket so it will listen for incoming connections */
    if (DEBUG) printf("SocketCellsWriter: Listening on port %d\n", port);
    if ((rc=listen(servSock, 1)) < 0) {
        fprintf(stderr, "ERROR; return code from listen() is %d\n", rc);
        exit(-1);
    }

    /* Set the size of the in-out parameter */
    clntLen = sizeof(echoClntAddr);

    /* Wait for a client to connect */
    if ((clntSock = accept(servSock, (struct sockaddr *) &echoClntAddr, &clntLen)) < 0){
        fprintf(stderr, "ERROR; return code from accept() is %d\n", clntSock);
        exit(-1);
    }

    /* clntSock is connected to a client! */

    fprintf(stderr, "Handling client %s\n", inet_ntoa(echoClntAddr.sin_addr));

    //HandleTCPClient(clntSock);

    ::close(servSock);

    this->socketfd = clntSock;
}
