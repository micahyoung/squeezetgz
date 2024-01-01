#!/bin/bash
set -o errexit -o pipefail -o nounset

dict="/usr/share/dict/words"
dictsize=$(wc -l < ${dict})

for i in {1..10}; do
    mkdir "${i}"
    
    split -l$((dictsize/i)) $dict "${i}/"
done