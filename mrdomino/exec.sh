#!/bin/bash

# The sole reason this script exists is because DominoUp CLI launcher does not
# accept interpreter name as its first parameter

exec python -m $*
