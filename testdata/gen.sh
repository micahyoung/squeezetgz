#!/bin/bash
set -o errexit -o pipefail -o nounset

cd "$(dirname "${0}")"

dict="/usr/share/dict/words"
dictsize=$(wc -l < ${dict})

for i in {2,4,6,10,20,100,1000}; do
    mkdir -p "${i}"
    
    split -l$((dictsize/i)) -a 3 $dict "${i}/"
done