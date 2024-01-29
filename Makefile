MEASUREMENTS?=${HOME}/measurements.txt

run:
	/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}"

format:
	mix format 1brc.exs
