# adjust the following;
# Where is the CDF library installed? 
cdfprefix=/usr/local
# From where can SQLite load extensions
sqlite_extpath=$(HOME)/code/sqlite/extensions

libcdfpath=$(cdfprefix)/cdf/lib

CC=gcc -g -O2

cdf.so: cdf.c
	$(CC) -L$(libcdfpath) -lcdf -fPIC -shared cdf.c -o cdf.so

install:
	mv cdf.so $(sqlite_extpath)
