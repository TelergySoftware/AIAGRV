CC = g++
INC = "/usr/include/python3.9/"
LIBS = -lboost_python39
SRCS = ICdataUtils.cc

compile: $(SRCS)
	@echo "Compiling..."
	${CC} ${SRCS} -Wall -Wextra -fPIC -shared -I ${INC} -o ./shared/ICdataUtils.so ${LIBS}
	@echo "Done"