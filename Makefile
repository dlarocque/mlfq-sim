# Author: Franklin Bristow
# Date: Summer 2022
# Notes: Students registered in COMP 3430 Summer 2022 have permission to 
# use/modify/submit this Makefile as part of their own lab/assignment work.

CFLAGS = -Wall -Wextra -Wpedantic -Werror -g
CC = clang
LDLIBS = -lpthread

.PHONY: clean

all: scheduler

scheduler: scheduler

clean:
	rm scheduler
