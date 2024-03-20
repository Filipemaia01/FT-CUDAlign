#!/bin/bash

./balancer 2 /dirs_MASA/work &
sleep 2
./controller config5m /dirs_MASA/work
