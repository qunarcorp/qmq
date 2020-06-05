test_get_time: \
	test_get_time.cpp \
	../src/TimeCalibrator.hpp
	g++ -g -std=c++17 test_get_time.cpp -I ../src -o test_get_time -luv

