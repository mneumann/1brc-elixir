MEASUREMENTS?=measurements.txt

run:
	/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}"
