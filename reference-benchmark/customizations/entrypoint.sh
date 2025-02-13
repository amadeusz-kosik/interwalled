#!/bin/bash

case "$1" in
  ailist)
    time /usr/src/AIList/bin/ailist "$2" "$3" > "$4"
    ;;

  aitree)
    time /usr/src/AIList/src_AITree/AITree "$2" "$3" > "$4"
    ;;

  nclist)
    echo "NCList is broken in this repository."
    exit 1
    ;;

  bedtools)
    time bedtools intersect -c -a "$2" -b "$3" > "$4"
    ;;

  *)
    echo "Unknown runner: $1"
    ;;
esac