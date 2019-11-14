test_http: test_http.cpp ../src/Http.hpp
	g++ -g -std=c++17 -I ../src -o test_http test_http.cpp -L /usr/lib/x86_64-linux-gnu -luv -lhttp_parser
