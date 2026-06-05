all: log_analyzer.c
	gcc log_analyzer.c -o log_analyzer -pthread

clean:
	rm -f log_analyzer

