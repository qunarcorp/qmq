test_meta_tcp: \
	test_meta_tcp.cpp \
	../src/MetaTcp.hpp \
	../src/Protocol.hpp \
	../src/Conn.hpp \
	../src/StackPool.hpp \
	../src/ExiledChecker.hpp
	g++ -g -std=c++17 -I ../src -o test_meta_tcp test_meta_tcp.cpp -L /usr/lib/x86_64-linux-gnu -luv
