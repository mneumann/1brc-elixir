MEASUREMENTS?=${HOME}/measurements.txt

run:
	/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}"

run-explorer:
	/usr/bin/time mix run --no-mix-exs 1brc-explorer.exs "${MEASUREMENTS}"

format:
	mix format 1brc.exs 1brc-explorer.exs
