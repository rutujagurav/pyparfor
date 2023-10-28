#!/bin/bash

N=(100 1000 10000 100000 1000000)
M=(1000000 100000 10000 1000 100)
echo "num_samples: $num_samples"

for i in "${!N[@]}"
    do
        for j in "${!M[@]}"
            do 
                n=${N[$i]}
                m=${M[$j]}
                echo "N: $n, M: $m"
                python -u -B timing_exps.py --N $n --M $m &> "timing_exps_N_${n}_M_${m}.log" &
                sleep 1
            done
    done
wait
