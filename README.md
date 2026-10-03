# Linux Concurrent Log Processor

This is a Linux C application for parallel scanning and analysis of a large number of log files. The application is designed to be highly concurrent, using fine grained locking to maximize performance on multi core systems.

## Key Implementations

1. Dynamically creating a dedicated scanner thread for each added directory to track file modification times using sys/stat.h.
2. Using a fixed pool of worker threads that concurrently process files from a shared queue and update global statistics.
3. Synchronizing data access using mutexes and condition variables to prevent double counting, data loss, and race conditions.
4. Implementing an interactive command line with real time statistics display.
5. Ensuring worker threads only process new or appended log entries by tracking the last processed sequence number per file.

## User Commands

1. add <dir>: Adds a new directory and starts a dedicated scanner thread for it.
2. stats: Prints the total number of read files, lines, and the count of ERROR, WARNING, and INFO logs.
3. top <N>: Displays the N most frequent log messages and their occurrence count.
4. watch: Starts a real time updating display of the system statistics, exited with CTRL+D.
5. stop: Gracefully shuts down the application and all active threads.

## Technologies

1. C programming language
2. Linux POSIX threads (pthread)
3. Mutexes and condition variables
4. Directory traversal (dirent.h) and file stat operations (sys/stat.h)
