#COMMIT=$(shell git describe head)
default : all
version : FORCE
	if [ -d ".git" ]; then git describe HEAD | tee $@; else if [ -f "$@" ]; then cat $@; else echo "$@ missing"; false; fi; fi
haproxyelastic : haproxyelastic.c common.o
	gcc -O3 -Wall -pedantic -Wextra -DCOMMIT=\"$(shell cat version)\" haproxyelastic.c -lGeoIP -o haproxyelastic -I/usr/local/include -L/usr/local/lib -lrt common.o
	#gcc -g -Wall -pedantic -Wextra -DCOMMIT=\"$(shell cat version)\" haproxyelastic.c -lGeoIP -o haproxyelastic -I/usr/local/include -L/usr/local/lib -lrt common.o
bcbs : bcbs.c common.o
	gcc -g -Wall -pedantic -Wextra -Wno-pointer-sign -DCOMMIT=\"$(shell cat version)\" -o bcbs bcbs.c common.o -lz -lrt
common.o : common.h common.c
	gcc -g -c -Wall -pedantic -Wextra -Wno-pointer-sign -o common.o common.c
all : version haproxyelastic bcbs
clean:
	rm -f *.o haproxyelastic bcbs
FORCE:
