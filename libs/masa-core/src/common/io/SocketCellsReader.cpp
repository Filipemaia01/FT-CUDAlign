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

#include "SocketCellsReader.hpp"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/socket.h> /* for socket(), bind(), and connect() */
#include <arpa/inet.h>  /* for sockaddr_in and inet_ntoa() */
#include <errno.h>
#include <netdb.h> //hostent

SocketCellsReader::SocketCellsReader(string hostname, int port, string signal_path, string shared_path) {
    this->hostname = hostname;
    this->port = port;
    this->socketfd = -1;
    this->signal_path = signal_path;
    this->close_socket_path = shared_path+"/closesocket.txt";
    this->failure_signal_path = shared_path+"/failure.txt";
    removeOldFiles();
    init();
}

SocketCellsReader::~SocketCellsReader() {
	close();
}

void SocketCellsReader::removeOldFiles() {
    //remove(signal_path.c_str());
    remove(close_socket_path.c_str());
    remove(failure_signal_path.c_str());
}

void SocketCellsReader::close() {
    sendFinishMessage();
    fprintf(stderr, "SocketCellsReader::close(): %d\n", socketfd);
    if (socketfd != -1) {
        ::close(socketfd);
        socketfd = -1;
    }
}

int SocketCellsReader::getType() {
	return INIT_WITH_CUSTOM_DATA;
}

void SocketCellsReader::failureSignal() {
    FILE* fd_failure;

    if(access(failure_signal_path.c_str(), F_OK)!=0) { //failure file was not created
        printf("Failure Signal Sent!\n");
        fd_failure = fopen(failure_signal_path.c_str(), "wb");
        //TODO: write the number of the failed GPU
        fclose(fd_failure);
    }
}

void SocketCellsReader::sendFinishMessage() {
    /* This function sends a message to the previous GPU sinalizing the end of it's execution
    *  The main goal of this is to detect the failure and identifying if it's indeed a failure
    *  or just the end of the execution
    */
    char verification[10]="finished";
    printf("Sending finished message to the previous GPU\n");
    send(socketfd, verification, 10, MSG_NOSIGNAL);
}

int SocketCellsReader::read(cell_t* buf, int len) {
    int pos=0, tries=3;//, tentativas = 0;
    FILE* end_exec_signal; FILE* close_socket;
    bool signalOk;

    while (pos < len*sizeof(cell_t)) {
    	int ret = recv(socketfd, (void*)(((unsigned char*)buf)+pos), len*sizeof(cell_t), 0);

    /* This region of the function was created in order to check the return of the recv function.
    *  If the return is 0, it means that nothing is being sent and, therefore, the socket may have been 
    *  disconnected. The GPU only blocks, waiting for a reconnection, if the GPU from which it receieves
    *  it's cells doesn't send any cells and if it hasn't ended it's execution(checks a signal file).
    */
        while(ret == 0 && tries > 0) { //if amount of bytes received is zero, most likely the socket has been closed
            ret = recv(socketfd, (void*)(((unsigned char*)buf)+pos), len*sizeof(cell_t), MSG_NOSIGNAL);
            tries --;
            printf("SCR: Trying %d \n", 3-tries);
            sleep(2);
        } 
        if (tries == 0) {
            end_exec_signal = fopen(this->signal_path.c_str(), "rt"); //file created when previous GPU finishes it's execution
            close_socket = fopen(this->close_socket_path.c_str(), "rt"); //file create when previous GPU closes socket connection

            signalOk = false;
            if (end_exec_signal!=NULL) {
                signalOk = true;
                fclose(end_exec_signal);
            }
            else if(close_socket!=NULL) {
                signalOk = true;
                fclose(close_socket);
            }
            if(!signalOk) {
                printf("Connection Lost!\n");
                ::close(socketfd);
                failureSignal();
                break;
            }
        }
        if (ret == -1) {
        	::close(socketfd);
            fprintf(stderr, "recv: Socket error -1\n");
            break;
        }
        pos += ret; 
    }
    return pos/sizeof(cell_t);
}

int SocketCellsReader::readInt(global_score_t* score) {
    int ret = recv(socketfd, (global_score_t*) score, sizeof(global_score_t), 0);
    if (ret == -1) {
       	close();
        fprintf(stderr, "recv: Socket error -1\n");
    }

    return ret;
}

void SocketCellsReader::init() {
    int rc;
    int sock;                        /* Socket descriptor */
    struct sockaddr_in echoServAddr; /* Echo server address */



    /* Create a reliable, stream socket using TCP */
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "ERROR creating socket: %s\n", strerror(errno));
		sleep(1);
	}

	/* Resolving DNS */
	char ip[100];
	if (resolveDNS(hostname.c_str(), ip)) {
        fprintf(stderr, "FATAL: cannot resolve hostname: %s\n", hostname.c_str());
        exit(-1);
	}
    /* Construct the server address structure */
    memset(&echoServAddr, 0, sizeof(echoServAddr));     /* Zero out structure */
    echoServAddr.sin_family      = AF_INET;             /* Internet address family */
    echoServAddr.sin_addr.s_addr = inet_addr(ip);   /* Server IP address */
    echoServAddr.sin_port        = htons(port); /* Server port */

    /* Establish the connection to the echo server */
    int max_retries = 3000;
    int retries = 0;
    int ok = 0;
    fprintf(stderr, "Listening on %s %d\n", hostname.c_str(), port);
    while ((retries < max_retries) && !ok) {
		if ((rc=connect(sock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr))) < 0) {
			if (retries % 100 == 0) {
				fprintf(stderr, "ERROR connecting to Server %s:%d [Retry %d/%d]. %s\n", ip, port,
						retries, max_retries, strerror(errno));
			}
			retries++;
			usleep(10000);
		} else {
			ok = 1;
		}
	}
	if (!ok) {
		fprintf(stderr, "ERROR connecting to Server. Aborting\n");
        failureSignal();
		exit(-1);
	}
    fprintf(stderr, "Connected to Server %s\n", inet_ntoa(echoServAddr.sin_addr));

    this->socketfd = sock;
}

int SocketCellsReader::resolveDNS(const char * hostname , char* ip) {
    struct hostent *he;
    struct in_addr **addr_list;
    int i;

    if ( (he = gethostbyname( hostname ) ) == NULL)
    {
        // get the host info
        herror("gethostbyname");
        return 1;
    }

    addr_list = (struct in_addr **) he->h_addr_list;

    for(i = 0; addr_list[i] != NULL; i++)
    {
        //Return the first one;
        strcpy(ip , inet_ntoa(*addr_list[i]) );
        return 0;
    }

    return 1;
}
