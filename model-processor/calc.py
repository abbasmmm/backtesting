import math

def phi(x):
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)

def pdf(x, mu=0.0, sigma=1.0):
    return phi((x - mu) / sigma) / sigma

def Phi(z):
    if z < -8.0: return 0.0
    if z >  8.0: return 1.0
    total = 0.0
    term = z
    i = 3
    while total != total + term:
        total += term
        term *= z * z / float(i)
        i += 2
    return 0.5 + total * phi(z)

def cdf(z, mu=0.0, sigma=1.0):
    return Phi((z - mu) / sigma)

def callPrice(s, x, r, sigma, t):
    a = (math.log(s/x) + (r + sigma * sigma/2.0) * t) / \
        (sigma * math.sqrt(t))
    b = a - sigma * math.sqrt(t)
    return s * cdf(a) - x * math.exp(-r * t) * cdf(b)