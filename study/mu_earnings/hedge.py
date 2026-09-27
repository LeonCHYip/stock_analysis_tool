import math
def N(x): return 0.5*(1+math.erf(x/math.sqrt(2)))
def bs(S,K,T,r,sig,kind):
    d1=(math.log(S/K)+(r+sig*sig/2)*T)/(sig*math.sqrt(T)); d2=d1-sig*math.sqrt(T)
    return (K*math.exp(-r*T)*N(-d2)-S*N(-d1)) if kind=="p" else (S*N(d1)-K*math.exp(-r*T)*N(d2))

S=975.26; r=0.04
print(f"MU ${S:.2f} | realised vol over the study window 69%\n")
print(f"{'hedge':<46}{'cost':>10}{'% of position':>15}{'annualised':>12}")
print("-"*83)
for lab,K,T,sig,rolls in [
    ("ATM put, 1 year",                     S,     1.0, 0.69, 1),
    ("10% OTM put, 1 year",                 S*0.9, 1.0, 0.69, 1),
    ("20% OTM put, 1 year",                 S*0.8, 1.0, 0.69, 1),
    ("ATM put, 3 months, rolled 4x/yr",     S,     0.25,0.69, 4),
    ("10% OTM put, 3 months, rolled 4x/yr", S*0.9, 0.25,0.69, 4),
]:
    p=bs(S,K,T,r,sig,"p"); tot=p*rolls
    print(f"{lab:<46}{p:>9.2f}{p/S*100:>14.1f}%{tot/S*100:>11.1f}%")

print("\nCollar — buy the 10% OTM put, sell a call to pay for it (1 year):")
put=bs(S,S*0.9,1.0,0.69,"p") if False else bs(S,S*0.9,1.0,r,0.69,"p")
for up in (1.15,1.25,1.40,1.60):
    call=bs(S,S*up,1.0,r,0.69,"c")
    net=put-call
    print(f"  put 10% OTM + short call {int((up-1)*100)}% OTM  ->  net {net:+8.2f} "
          f"({net/S*100:+5.1f}% of position)   upside capped at +{int((up-1)*100)}%")

print("\nWhat capping the upside would have cost, this window:")
print("  MU actual, Mar-2024 -> Sep-2026:  +913%  (10.13x)")
print("  same period capped at +40%/yr:    ~+175% (2.75x) -- the return WAS the upside tail")
