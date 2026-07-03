# Ρυθμίσεις Compiler 
CC = gcc
CFLAGS = -I/usr/include/cjson
LDFLAGS = -lwebsockets -lcjson -lpthread

TARGET = telemetry_system
SRC = main2.c

# make
all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(SRC) -o $(TARGET) $(LDFLAGS) $(CFLAGS)
	@echo "Το compile ολοκληρώθηκε επιτυχώς!"

# make clean
clean:
	rm -f $(TARGET) events.log
	@echo " Τα παλιά αρχεία διαγράφηκαν."

# Διαχείριση του Service (make restart) 
restart:
	sudo systemctl restart telemetry.service
	@echo " Το service επανεκκινήθηκε!"

# Προβολή της κατάστασης (make status) 
status:
	sudo systemctl status telemetry.service