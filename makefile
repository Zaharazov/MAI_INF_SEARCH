CC = g++
CFLAGS = -std=c++17 `pkg-config --cflags libmongocxx`
LIBS = `pkg-config --libs libmongocxx`

search: main.cpp
	$(CC) $(CFLAGS) main.cpp -o search_engine $(LIBS)

clean:
	rm -f search_engine