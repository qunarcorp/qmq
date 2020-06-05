test_conn: test_conn.cpp ../src/Conn.hpp
	g++ -g -std=c++17 -I ../src -o test_conn test_conn.cpp -L /usr/lib/x86_64-linux-gnu -luv -lhttp_parser
