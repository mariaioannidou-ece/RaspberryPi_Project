
# Jetstream Firehose Real-Time Telemetry 🚀

This repository contains the final project for the **Real-Time Embedded Systems** course at the Department of Electrical and Computer Engineering (ECE), Aristotle University of Thessaloniki (AUTh).

## 📌 Project Overview
A multithreaded embedded system developed entirely in **C (User Space)** for the **Raspberry Pi Zero 2 W**. The system is designed to asynchronously fetch, parse, and log real-time data from the Bluesky Jetstream Firehose WebSocket API. 

The core architecture utilizes the **Producer-Consumer** design pattern implemented via POSIX Threads (`pthreads`), ensuring zero packet loss and high network resilience.

## ⚙️ System Architecture
The application is divided into three distinct threads:
* **Producer Thread:** Handles the asynchronous WebSocket network communication using `libwebsockets`. It receives raw JSON frames and stores them in a Bounded Circular Queue (Ring Buffer).
* **Consumer Thread:** An event-driven thread that wakes up to parse the JSON data using `cJSON`, extracts the message categories, and safely updates global counters using mutexes (`pthread_mutex_t`).
* **Monitor Thread:** A strictly periodic thread that wakes up exactly every 1 second using `clock_nanosleep()` and `TIMER_ABSTIME` to avoid clock drift. It logs the message rates, buffer occupancy, and CPU usage into a CSV file.

## 📁 Repository Contents
* `main2.c`: The complete source code of the telemetry system.
* `Makefile`: The build automation file for compiling the code on the Raspberry Pi.
* `metrics_log.txt`: The official dataset collected over a continuous 24-hour physical execution on the Raspberry Pi, containing over 86,400 precise log entries.

## 🛠️ Technologies & Libraries Used
* Language: C (POSIX Standards)
* Multithreading: `pthreads` (Mutexes, Condition Variables)
* Networking: `libwebsockets`
* JSON Parsing: `cJSON`
