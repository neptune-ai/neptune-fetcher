#!/bin/bash
for var in $(env | grep ^a) ; do echo $var | python3 -c 'import binascii, sys ; print(binascii.hexlify(sys.stdin.read().encode()))' ; done
