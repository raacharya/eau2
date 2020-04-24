a.out:
	g++ -std=c++11 test/test.cpp

test:
	./a.out

wc.out:
	g++ -std=c++11 test/wc.cpp -o wc.out

run:
	./wc.out -f "100k.txt"

clean:
	rm a.out
	rm wc.out