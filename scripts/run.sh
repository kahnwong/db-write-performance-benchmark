#!/bin/bash

N_ROWS=50000 make 02-start-experiment
N_ROWS=100000 make 02-start-experiment
N_ROWS=1000000 make 02-start-experiment # pandas dead here
N_ROWS=5000000 make 02-start-experiment
N_ROWS=10000000 make 02-start-experiment
N_ROWS=20000000 make 02-start-experiment
N_ROWS=50000000 make 02-start-experiment
