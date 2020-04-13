a.out:
	g++ -std=c++11 test/test.cpp

test: a.out
	./a.out

clean:
	rm a.out