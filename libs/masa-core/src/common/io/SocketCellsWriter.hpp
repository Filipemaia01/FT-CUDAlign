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

#ifndef SOCKETCELLSWRITER_HPP_
#define SOCKETCELLSWRITER_HPP_

#include "CellsWriter.hpp"
#include <string>
using namespace std;

class SocketCellsWriter: public CellsWriter {
public:
	SocketCellsWriter(string hostname, int port, string shared_path);
	virtual ~SocketCellsWriter();
	virtual void close();

	virtual int write(const cell_t* buf, int len);
	virtual int writeInt(global_score_t* score);
private:
    string hostname;
	string close_socket_path;
	string failure_signal_path;
    int port;
    int socketfd;

    void init();
	int isopen(int socket);
	void waitForFinishMessage();
	void failureSignal();
};

#endif /* SOCKETCELLSWRITER_HPP_ */
