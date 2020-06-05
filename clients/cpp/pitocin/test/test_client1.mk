CLIENT_C=	../src/Client.cpp

CLIENT_H=	../src/Client.hpp \
			../src/Http.hpp \
			../src/Conn.hpp \
			../src/StackPool.hpp \
			../src/Protocol.hpp

test_conn: test_client1.cpp ${CLIENT_C} ${CLIENT_H}
	g++ -g -std=c++17 -I ../src -o test_client1 test_client1.cpp ${CLIENT_C} -L /usr/lib/x86_64-linux-gnu -luv -lhttp_parser
