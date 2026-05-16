# Deliberately incorrect: prints a*b instead of a+b.
# Expected results vs canonical hash of a+b:
#   ordinal 1: 2*3=6   != 5   → WRONG_ANSWER
#   ordinal 2: 10*20=200 != 30 → WRONG_ANSWER
#   ordinal 3: -1*1=-1 != 0  → WRONG_ANSWER
#   ordinal 4: 100*200=20000 != 300 → WRONG_ANSWER
#   ordinal 5: 0*0=0   == 0  → ACCEPTED  ← single ordinal happens to match
# Worker aggregates per-ordinal results → overall_verdict = WRONG_ANSWER.
import sys

a, b = map(int, sys.stdin.read().split())
print(a * b)
