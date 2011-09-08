# copyright(c)Liexusong 280259971@qq.com
# Date: 2011/10/12
# Author: Liexusong

OBJ = ae.o slabs.o dlist.o squirrel.o

all:$(OBJ)
	gcc -o squirrel $(OBJ) -llua -lpthread -lm -ldl
	@echo ""
	@echo "+--------------------------------------------+"
	@echo "|            Finish compilation              |"
	@echo "+--------------------------------------------+"
	@echo "|         Thanks using SquirrelMQ            |"
	@echo "|   copyright(c)Liexusong 280259971@qq.com   |"
	@echo "+--------------------------------------------+"

ae.o: ae.c ae.h
	gcc -c ae.c
slabs.o: slabs.c slabs.h
	gcc -c slabs.c
dlist.o: dlist.c dlist.h
	gcc -c dlist.c
squirrel.o: squirrel.c ae.c ae.h slabs.c slabs.h dlist.c dlist.h
	gcc -c squirrel.c
